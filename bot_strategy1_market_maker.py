"""
Strategy 1 — Polymarket CLOB Market Maker (UP+DOWN)
===================================================

Objetivo:
- Operar os dois lados (UP e DOWN) no mesmo mercado para capturar spread/mean-reversion.
- Controlar risco de inventário, latência e exposição por mercado.
- Ajustar agressividade via modelo online simples (bandit-like) com atualização contínua.

IMPORTANTE:
- Use em DRY_RUN primeiro.
- Requer credenciais no ambiente (.env):
  POLYMARKET_PRIVATE_KEY, POLYMARKET_FUNDER_ADDRESS, POLYMARKET_SIGNATURE_TYPE
"""

import os
import json
import time
import math
import threading
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple

import requests
from dotenv import load_dotenv

try:
    import websocket
    HAS_WEBSOCKET = True
except Exception:
    HAS_WEBSOCKET = False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot_strategy1_mm.log", encoding="utf-8")],
)
log = logging.getLogger("strategy1-mm")


@dataclass
class MMConfig:
    gamma_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    binance_trade_ws: str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

    dry_run: bool = True

    # Discovery/loop
    loop_ms: int = 800
    quote_refresh_ms: int = 700
    cancel_stale_after_s: float = 4.0

    # Quote logic
    min_edge_bps: float = 80.0       # edge mínimo para cotar
    max_spread_pay: float = 0.12     # ignora mercado com spread gigante
    base_order_usdc: float = 8.0
    max_order_usdc: float = 20.0

    # Inventory risk
    max_inventory_shares: float = 120.0
    inventory_soft_limit: float = 70.0
    inventory_penalty_bps: float = 120.0

    # Global risk
    max_daily_loss: float = 60.0
    max_open_orders: int = 8
    max_notional_live: float = 220.0

    # "ML" online (contextual weight)
    ml_enabled: bool = True
    ml_lr: float = 0.08
    ml_clip: float = 2.5

    # creds
    private_key: str = ""
    funder_address: str = ""
    signature_type: int = 2

    def __post_init__(self):
        self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY", self.private_key)
        self.funder_address = os.getenv("POLYMARKET_FUNDER_ADDRESS", self.funder_address)
        self.signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", str(self.signature_type)))
        self.dry_run = os.getenv("DRY_RUN", str(self.dry_run).lower()).lower() in ("1", "true", "yes")


@dataclass
class MarketRef:
    slug: str
    end_ts: int
    up_token: str
    down_token: str


class OnlineEdgeModel:
    """Modelo online simples: score = w·x; ajusta edge alvo por contexto."""

    def __init__(self, lr: float = 0.08, clip: float = 2.5, path: str = "strategy1_model.json"):
        self.lr = lr
        self.clip = clip
        self.path = path
        self.w = {
            "spread": 0.2,
            "book_imb": 0.2,
            "trade_imb": 0.2,
            "vol": 0.2,
            "time": 0.2,
        }
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if isinstance(d.get("w"), dict):
                    for k in self.w.keys():
                        if k in d["w"]:
                            self.w[k] = float(d["w"][k])
                log.info("🧠 OnlineEdgeModel carregado")
        except Exception as e:
            log.debug(f"Model load error: {e}")

    def _save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"w": self.w, "updated": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        except Exception:
            pass

    def score(self, x: Dict[str, float]) -> float:
        with self._lock:
            return sum(self.w[k] * float(x.get(k, 0.0)) for k in self.w.keys())

    def update(self, x: Dict[str, float], reward: float):
        r = max(-self.clip, min(self.clip, reward))
        with self._lock:
            for k in self.w.keys():
                self.w[k] += self.lr * r * float(x.get(k, 0.0))
            self._save()


class BinanceTape:
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.buy_vol = 0.0
        self.sell_vol = 0.0
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        if not HAS_WEBSOCKET:
            log.warning("⚠ websocket-client ausente")
            return
        self._running = True
        threading.Thread(target=self._run, daemon=True, name="TapeWS").start()

    def stop(self):
        self._running = False

    def imbalance(self) -> float:
        with self._lock:
            tot = self.buy_vol + self.sell_vol
            if tot <= 0:
                return 0.0
            return (self.buy_vol - self.sell_vol) / tot

    def decay(self):
        with self._lock:
            self.buy_vol *= 0.92
            self.sell_vol *= 0.92

    def _on_message(self, _ws, raw: str):
        try:
            d = json.loads(raw)
            qty = float(d.get("q", 0))
            is_taker_buy = not bool(d.get("m", False))
            with self._lock:
                if is_taker_buy:
                    self.buy_vol += qty
                else:
                    self.sell_vol += qty
        except Exception:
            pass

    def _run(self):
        reconnects = 0
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self._on_message,
                    on_open=lambda _w: log.info(f"✅ Tape WS conectado ({reconnects} reconexões)"),
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.debug(f"Tape WS error: {e}")
            reconnects += 1
            time.sleep(1)


class PolymarketMMClient:
    def __init__(self, cfg: MMConfig):
        self.cfg = cfg
        self.client = None
        if not cfg.dry_run:
            self._init_client()

    def _init_client(self):
        if not self.cfg.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY ausente")
        from py_clob_client.client import ClobClient

        self.client = ClobClient(
            host=self.cfg.clob_host,
            key=self.cfg.private_key,
            chain_id=self.cfg.chain_id,
            signature_type=self.cfg.signature_type,
            funder=self.cfg.funder_address,
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

    def get_book(self, token_id: str) -> dict:
        if self.client:
            try:
                return self.client.get_book(token_id=token_id)
            except Exception:
                pass
        try:
            return requests.get(f"{self.cfg.clob_host}/book", params={"token_id": token_id}, timeout=2).json()
        except Exception:
            return {}

    def place_limit(self, token_id: str, side: str, price: float, size: float) -> str:
        if self.cfg.dry_run:
            oid = f"DRY_{int(time.time()*1000)}"
            log.info(f"🏃 DRY {side} {size:.2f}@{price:.2f} {token_id[:8]}..")
            return oid
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        side_const = BUY if side.upper() == "BUY" else SELL
        order = self.client.create_order(OrderArgs(token_id=token_id, price=price, size=size, side=side_const))
        resp = self.client.post_order(order, OrderType.GTC)
        if isinstance(resp, dict):
            return resp.get("orderID") or resp.get("order_id") or resp.get("id") or ""
        return ""

    def cancel(self, order_id: str):
        if self.cfg.dry_run or not self.client or not order_id:
            return
        try:
            self.client.cancel(order_id=order_id)
        except Exception:
            pass


def now_5m_ts() -> int:
    now = int(time.time())
    return (now // 300) * 300


def find_current_market(cfg: MMConfig) -> Optional[MarketRef]:
    for off in [0, -300]:
        slug = f"btc-updown-5m-{now_5m_ts() + off}"
        try:
            r = requests.get(
                f"{cfg.gamma_host}/markets",
                params={"slug": slug, "active": "true", "closed": "false"},
                timeout=5,
            )
            d = r.json()
            if not d:
                continue
            m = d[0] if isinstance(d, list) else d
            if m.get("closed") or not m.get("active"):
                continue
            tids = json.loads(m.get("clobTokenIds", "[]"))
            if len(tids) < 2:
                continue
            outcomes = m.get("outcomes", ["Up", "Down"])
            if isinstance(outcomes, str):
                outcomes = [x.strip() for x in outcomes.split(",")]
            up_i = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
            dn_i = next((i for i, o in enumerate(outcomes) if str(o).lower() == "down"), 1)
            end_ts = 0
            if m.get("endDate"):
                end_ts = int(datetime.fromisoformat(m["endDate"].replace("Z", "+00:00")).timestamp())
            return MarketRef(slug=slug, end_ts=end_ts, up_token=tids[up_i], down_token=tids[dn_i])
        except Exception:
            continue
    return None


class Strategy1MarketMaker:
    def __init__(self, cfg: MMConfig):
        self.cfg = cfg
        self.tape = BinanceTape(cfg.binance_trade_ws)
        self.model = OnlineEdgeModel(lr=cfg.ml_lr, clip=cfg.ml_clip)
        self.clob = PolymarketMMClient(cfg)

        self.market: Optional[MarketRef] = None
        self.open_orders: Dict[str, dict] = {}
        self.inventory = {"UP": 0.0, "DOWN": 0.0}
        self.realized_pnl = 0.0

        self._running = True
        self._last_quote_ts = 0.0

    def _book_stats(self, token_id: str) -> Tuple[float, float, float, float]:
        b = self.clob.get_book(token_id)
        bids = b.get("bids", []) or []
        asks = b.get("asks", []) or []
        best_bid = max((float(x["price"]) for x in bids), default=0.0)
        best_ask = min((float(x["price"]) for x in asks), default=1.0)
        spread = max(0.0, best_ask - best_bid)
        bid_sz = sum(float(x.get("size", 0)) for x in bids[:5])
        ask_sz = sum(float(x.get("size", 0)) for x in asks[:5])
        imb = (bid_sz - ask_sz) / (bid_sz + ask_sz) if (bid_sz + ask_sz) > 0 else 0.0
        return best_bid, best_ask, spread, imb

    def _inventory_penalty(self, side: str) -> float:
        inv = self.inventory.get(side, 0.0)
        limit = self.cfg.inventory_soft_limit
        if abs(inv) <= limit:
            return 0.0
        excess = min(1.0, (abs(inv) - limit) / max(1.0, self.cfg.max_inventory_shares - limit))
        return excess * self.cfg.inventory_penalty_bps

    def _context_score(self, spread: float, imb: float, side: str, tr_s: int) -> float:
        trade_imb = self.tape.imbalance()
        x = {
            "spread": max(-1.0, min(1.0, (spread - 0.02) / 0.08)),
            "book_imb": imb if side == "UP" else -imb,
            "trade_imb": trade_imb if side == "UP" else -trade_imb,
            "vol": min(1.0, abs(trade_imb) * 2.0),
            "time": max(0.0, min(1.0, (15.0 - tr_s) / 15.0)),
        }
        return self.model.score(x)

    def _compute_quote(self, side: str, bid: float, ask: float, spread: float, imb: float, tr_s: int) -> Tuple[float, float, float]:
        if spread <= 0 or spread > self.cfg.max_spread_pay:
            return 0.0, 0.0, 0.0

        ml_score = self._context_score(spread, imb, side, tr_s) if self.cfg.ml_enabled else 0.0
        target_edge_bps = self.cfg.min_edge_bps - ml_score * 25.0
        target_edge_bps += self._inventory_penalty(side)
        target_edge_bps = max(30.0, min(220.0, target_edge_bps))

        half = spread / 2.0
        micro = target_edge_bps / 10000.0
        mid = (bid + ask) / 2.0

        buy_px = round(max(0.01, min(0.99, mid - half + micro)), 2)
        sell_px = round(max(0.01, min(0.99, mid + half - micro)), 2)
        edge = max(0.0, (sell_px - buy_px) * 10000)
        return buy_px, sell_px, edge

    def _place_pair(self, token: str, side_key: str, buy_px: float, sell_px: float, usdc: float):
        if buy_px <= 0 or sell_px <= 0 or sell_px <= buy_px:
            return
        size = round(max(5.0, usdc / max(buy_px, 0.01)), 2)

        if len(self.open_orders) >= self.cfg.max_open_orders:
            return

        boid = self.clob.place_limit(token, "BUY", buy_px, size)
        soid = self.clob.place_limit(token, "SELL", sell_px, size)
        ts = time.time()
        if boid:
            self.open_orders[boid] = {"ts": ts, "token": token, "side": side_key, "dir": "BUY", "px": buy_px, "size": size}
        if soid:
            self.open_orders[soid] = {"ts": ts, "token": token, "side": side_key, "dir": "SELL", "px": sell_px, "size": size}

        log.info(f"🧩 {side_key} quote | buy={buy_px:.2f} sell={sell_px:.2f} size={size:.2f}")

    def _cancel_stale(self):
        now = time.time()
        for oid, od in list(self.open_orders.items()):
            if now - od["ts"] > self.cfg.cancel_stale_after_s:
                self.clob.cancel(oid)
                self.open_orders.pop(oid, None)

    def _mark_to_model_reward(self):
        """Recompensa simples: spread capturável - penalidade inventário."""
        inv_pen = (abs(self.inventory["UP"]) + abs(self.inventory["DOWN"])) / max(1.0, self.cfg.max_inventory_shares)
        reward = 0.15 - inv_pen
        x = {"spread": 0.1, "book_imb": 0.0, "trade_imb": self.tape.imbalance(), "vol": abs(self.tape.imbalance()), "time": 0.2}
        self.model.update(x, reward)

    def _risk_ok(self) -> bool:
        if self.realized_pnl <= -self.cfg.max_daily_loss:
            log.warning("🛑 stop diário atingido")
            return False
        notional = sum(abs(v["size"] * v["px"]) for v in self.open_orders.values())
        if notional > self.cfg.max_notional_live:
            log.warning(f"⚠ notional alto: {notional:.2f}")
            return False
        if abs(self.inventory["UP"]) > self.cfg.max_inventory_shares or abs(self.inventory["DOWN"]) > self.cfg.max_inventory_shares:
            log.warning("⚠ inventário excedido")
            return False
        return True

    def _sync_market(self):
        m = find_current_market(self.cfg)
        if not m:
            return
        if self.market is None or self.market.slug != m.slug:
            self.market = m
            self.open_orders.clear()
            self.inventory = {"UP": 0.0, "DOWN": 0.0}
            log.info(f"🆕 Market: {m.slug}")

    def run(self):
        self.tape.start()
        log.info("🚀 Strategy1 MM iniciado")

        while self._running:
            try:
                self._sync_market()
                if not self.market:
                    time.sleep(1)
                    continue

                tr_s = max(0, self.market.end_ts - int(time.time())) if self.market.end_ts else 10
                fast_zone = tr_s <= 20

                if not self._risk_ok():
                    self._cancel_stale()
                    time.sleep(self.cfg.loop_ms / 1000.0)
                    continue

                if (time.time() - self._last_quote_ts) * 1000.0 >= self.cfg.quote_refresh_ms:
                    up_bid, up_ask, up_spread, up_imb = self._book_stats(self.market.up_token)
                    dn_bid, dn_ask, dn_spread, dn_imb = self._book_stats(self.market.down_token)

                    # Quote UP token
                    up_buy, up_sell, up_edge = self._compute_quote("UP", up_bid, up_ask, up_spread, up_imb, tr_s)
                    # Quote DOWN token
                    dn_buy, dn_sell, dn_edge = self._compute_quote("DOWN", dn_bid, dn_ask, dn_spread, dn_imb, tr_s)

                    usdc = min(self.cfg.max_order_usdc, self.cfg.base_order_usdc * (1.4 if fast_zone else 1.0))

                    if up_edge >= self.cfg.min_edge_bps:
                        self._place_pair(self.market.up_token, "UP", up_buy, up_sell, usdc)
                    if dn_edge >= self.cfg.min_edge_bps:
                        self._place_pair(self.market.down_token, "DOWN", dn_buy, dn_sell, usdc)

                    self._last_quote_ts = time.time()

                self._cancel_stale()
                self._mark_to_model_reward()
                self.tape.decay()
                time.sleep(self.cfg.loop_ms / 1000.0)
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"loop error: {e}", exc_info=True)
                time.sleep(1)

        self.tape.stop()
        log.info("🛑 Strategy1 MM finalizado")


if __name__ == "__main__":
    Strategy1MarketMaker(MMConfig()).run()
