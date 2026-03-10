"""
╔══════════════════════════════════════════════════════════════════════════╗
║  POLYMARKET BTC LAST-SECOND REVERSAL BOT v3.1                          ║
║  ─────────────────────────────────────────────────────────────────────  ║
║  Detecta pressão de volume na Binance ANTES do Chainlink atualizar      ║
╚══════════════════════════════════════════════════════════════════════════╝

ESTRATÉGIA CORE: Last-Second Volume Reversal
════════════════════════════════════════════

O Chainlink (oráculo do Polymarket) tem delay de ~2-5s vs Binance.

Cenário que exploramos:
  T-8s  → BTC abaixo do target, parece ir DOWN
  T-6s  → Whale inicia compra massiva na Binance
  T-6s  → Nós detectamos volume em ~150ms e apostamos UP
  T-3s  → Chainlink confirma BTC acima → UP ganha

LATÊNCIA (após fixes v3.1):
  Trade acontece na Binance:          t = 0
  WebSocket entrega na _on_message:   t ≈ +60ms
  Signal detectado (event-driven):    t ≈ +61ms   ← sem polling delay
  Ordem submetida (sem REST extra):   t ≈ +300ms  ← usa preço cacheado
  ──────────────────────────────────────────────
  TOTAL:                              t ≈ 300-400ms

  vs Chainlink delay: 2000-5000ms → edge de 1.6s a 4.7s

MELHORIAS v3.1 vs v3.0:
  [FIX 1] Event-driven: detecção em _on_message (não polling de 100ms)
  [FIX 2] Odds UP+DOWN buscadas em paralelo (threads simultâneas)
  [FIX 3] Ask price cacheado no OddsCache → executor sem REST extra
  [FIX 4] Sleep mínimo no hot path: 20ms no loop backup
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, Callable, Literal
from collections import deque

import requests
from dotenv import load_dotenv

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_v3.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("polybot")


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # ── Reversal (estratégia principal) ──
    reversal_start: int = 9            # Começa scanning a T-9s
    reversal_end: int = 1              # Para a T-2s
    buy_pressure_thresh: float = 0.70  # 70% taker buys = bullish
    min_vol_surge: float = 1.8         # Surge mínimo: 1.8x a média de 20s
    min_price_vel: float = 0.0015      # Mínimo 0.0015%/s
    max_reversal_delta: float = 0.20   # BTC dentro de ±0.20% do target

    # ── Filtro combo δ × volume ──────────────────────────────────────────────
    weak_combo_delta: float = 0.08    # Se abs_delta ≥ este valor (0.08%)...
    weak_combo_surge: float = 2.5     # ...exige no mínimo este vol_surge (2.5x)
    #   Lógica: distância grande do target SÓ é apostável se o volume confirmar.
    #   Exemplo: δ=0.12%, surge=1.9x → skip ❌  (precisa mover muito com volume fraco)
    #            δ=0.12%, surge=3.1x → ok  ✅  (volume forte confirma o movimento)
    #            δ=0.03%, surge=1.9x → ok  ✅  (distância pequena, qualquer volume serve)

    # ── Momentum (fallback) ──
    momentum_start: int = 10           # era 15 — janela reduzida (menos tempo para reverter)
    momentum_end: int = 5
    min_momentum_delta: float = 0.015
    momentum_buy_thresh: float = 0.62  # era 0.52 — exige convicção real de compra/venda
    min_momentum_vol_surge: float = 1.5  # novo — surge mínimo para momentum não disparar no vazio

    # ── Execução ──
    base_bet: float = 5.0
    min_bet: float = 5.0
    max_bet: float = 20.0
    max_odd: float = 0.95
    max_late_odd: float = 0.87  # odds máximas antes de considerar "já precificado" pelo mercado

    # ── Book Imbalance (CLOB) ──────────────────────────────────────────────
    use_book_imbalance: bool = True
    max_book_imbalance_against: float = 0.30  # skip se imbalance > 0.30 CONTRA a direção do sinal
    # Exemplo: apostando UP, mas book_imbalance = -0.40 (40% mais liquidez DOWN) → skip

    # ── Trade Rate (confirmação de whale real) ────────────────────────────
    min_trade_rate: float = 3.0  # mínimo de trades/s durante o surge para confirmar atividade real

    # ── Janela de Confirmação do Sinal ────────────────────────────────────
    signal_confirm_ms: float = 120.0       # T >= 4s: confirmação normal
    signal_confirm_ms_urgent: float = 60.0  # T <= 3s: confirmação reduzida (tempo escasso)

    # ── Scalp (saída antecipada para garantir lucro) ─────────────────────
    enable_scalp: bool = True
    scalp_exit_odd: float = 0.92    # vende quando odds da nossa posição ≥ este valor
    scalp_min_time: int = 3         # não tenta sair se sobrar menos de N segundos
    scalp_poll_ms: float = 150.0    # frequência de checagem de odds pós-entrada (ms)

    # ── Cooldown por fonte ────────────────────────────────────────────────
    max_consecutive_momentum_losses: int = 2  # losses seguidos de MOMENTUM antes de cooldown
    momentum_cooldown_markets: int = 3        # mercados que MOMENTUM fica suspenso

    # ── Risco ──
    max_daily_loss: float = 50.0
    max_consecutive_losses: int = 6
    cooldown_markets: int = 2

    # ── APIs ──
    gamma_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"
    binance_trade_ws: str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    binance_kline_url: str = "https://api.binance.com/api/v3/klines"
    chain_id: int = 137

    # ── Credenciais ──
    private_key: str = "f28cb3270d6c4a9a02a54aa567948f485773c5a7a1b19588478b2bdda2f5777b"
    funder_address: str = "0xcd30786A7546807172050C6F4295F2CE292D943c"
    signature_type: int = 2
    dry_run: bool = False   

    def __post_init__(self):
        # IMPORTANTE: usa self.* como fallback para não sobrescrever valores hardcoded
        # quando a variável de ambiente não está definida no .env
        self.private_key     = os.getenv("POLYMARKET_PRIVATE_KEY",     self.private_key)
        self.funder_address  = os.getenv("POLYMARKET_FUNDER_ADDRESS",  self.funder_address)
        self.signature_type  = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", str(self.signature_type)))
        self.dry_run         = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
        # Remover prefixo 0x da chave privada se presente (py-clob-client não aceita)
        if self.private_key and self.private_key.startswith("0x"):
            self.private_key = self.private_key[2:]
        self.base_bet         = float(os.getenv("BET_AMOUNT",         str(self.base_bet)))
        self.max_bet          = float(os.getenv("MAX_BET",            str(self.max_bet)))
        self.max_daily_loss   = float(os.getenv("MAX_DAILY_LOSS",     str(self.max_daily_loss)))
        self.buy_pressure_thresh = float(os.getenv("BUY_PRESSURE_THRESH", str(self.buy_pressure_thresh)))
        self.min_vol_surge    = float(os.getenv("MIN_VOL_SURGE",      str(self.min_vol_surge)))


# ═══════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    timestamp: float       # Unix segundos (float)
    price: float
    quantity: float        # BTC
    is_taker_buy: bool     # True = buyer foi agressivo (m=False no Binance stream)


@dataclass
class MarketInfo:
    market_id: str
    slug: str
    question: str
    end_timestamp: int
    up_token_id: str
    down_token_id: str
    price_to_beat: float = 0.0
    gamma_up: float = 0.5
    gamma_down: float = 0.5


@dataclass
class OddsSnapshot:
    up: float
    down: float
    spread: float = 0.0
    up_liquidity: float = 0.0
    down_liquidity: float = 0.0
    book_imbalance: float = 0.0
    # [FIX 3] Ask prices cacheados para execução sem REST extra
    up_ask: float = 0.95    # Melhor ask do token UP (preço de compra)
    down_ask: float = 0.95  # Melhor ask do token DOWN
    source: str = "UNKNOWN"
    timestamp: float = 0.0


@dataclass
class Signal:
    direction: Literal["UP", "DOWN"]
    confidence: float
    source: str
    delta_pct: float
    buy_pressure: float
    vol_surge: float
    price_vel: float
    time_remaining: int
    bet_size: float = 5.0
    entry_odd: float = 0.5


@dataclass
class BetRecord:
    timestamp: float
    market_slug: str
    side: Literal["UP", "DOWN"]
    amount: float
    entry_odd: float
    delta_pct: float
    confidence: float
    time_remaining: int
    source: str
    buy_pressure: float
    vol_surge: float
    price_vel: float
    price_at_entry: float
    price_to_beat: float
    # Token IDs para verificar resultado direto no CLOB (não no Binance)
    up_token_id: str = ""
    down_token_id: str = ""
    result: Optional[str] = None
    profit: float = 0.0
    # ── Fill real da CLOB (preenchido pelo ResultChecker) ──────────────
    order_id: str = ""          # ID da ordem na CLOB
    actual_size: float = 0.0   # shares realmente comprados
    actual_price: float = 0.0  # preço médio real do fill
    # ── Scalp ──────────────────────────────────────────────────────────
    scalp_executed: bool = False  # True = saiu antecipado via scalp
    scalp_profit: float = 0.0    # lucro garantido pelo scalp
    actual_cost: float = 0.0   # USDC realmente gastos (size * price)


# ═══════════════════════════════════════════════════════════════════════
# TRADE BUFFER — Buffer circular thread-safe de trades individuais
# ═══════════════════════════════════════════════════════════════════════

class TradeBuffer:
    """
    Armazena cada trade individual da Binance com seu timestamp e direção.

    Métricas (todas em O(n) sobre a janela, tipicamente < 500 items):
      buy_pressure(t)    → % do BTC volume que foi taker buy em t segundos
      vol_surge_ratio()  → volume recente vs baseline (detecção de whale)
      price_velocity(t)  → variação de preço por segundo (%/s)
      trade_rate(t)      → trades por segundo (proxy de atividade)
    """

    def __init__(self, history_seconds: float = 60.0):
        self._trades: deque = deque()
        self._history = history_seconds
        self._lock = threading.Lock()
        self._latest_price: float = 0.0
        self._last_ts: float = 0.0

    def add(self, trade: Trade):
        cutoff = trade.timestamp - self._history
        with self._lock:
            self._trades.append(trade)
            self._latest_price = trade.price
            self._last_ts = trade.timestamp
            while self._trades and self._trades[0].timestamp < cutoff:
                self._trades.popleft()

    def latest_price(self) -> float:
        return self._latest_price

    def is_data_fresh(self, max_age: float = 2.0) -> bool:
        return (time.time() - self._last_ts) < max_age if self._last_ts > 0 else False

    def _window(self, seconds: float) -> list:
        cutoff = time.time() - seconds
        with self._lock:
            return [t for t in self._trades if t.timestamp >= cutoff]

    def buy_pressure(self, seconds: float) -> tuple:
        """(buy_ratio, total_btc_volume) na janela de t segundos."""
        window = self._window(seconds)
        if not window:
            return 0.5, 0.0
        buy_vol = sum(t.quantity for t in window if t.is_taker_buy)
        total_vol = sum(t.quantity for t in window)
        return (buy_vol / total_vol if total_vol > 0 else 0.5), total_vol

    def volume_rate(self, seconds: float) -> float:
        window = self._window(seconds)
        return sum(t.quantity for t in window) / seconds if seconds > 0 else 0.0

    def trade_rate(self, seconds: float) -> float:
        return len(self._window(seconds)) / seconds if seconds > 0 else 0.0

    def vol_surge_ratio(self, short_s: float = 3.0, baseline_s: float = 20.0) -> float:
        """Volume rate recente vs baseline. > 1 = surge (whale ativo)."""
        short = self.volume_rate(short_s)
        base = self.volume_rate(baseline_s)
        return short / base if base > 0 else 1.0

    def price_velocity(self, seconds: float) -> float:
        """Variação de preço por segundo (%) na janela. + = subindo, - = caindo."""
        window = self._window(seconds)
        if len(window) < 2:
            return 0.0
        p_old, p_new = window[0].price, window[-1].price
        elapsed = window[-1].timestamp - window[0].timestamp
        if elapsed <= 0.0 or p_old <= 0.0:
            return 0.0
        return (p_new - p_old) / p_old * 100.0 / elapsed

    def price_acceleration(self) -> float:
        """Compara velocidade dos últimos 2s vs 2-4s atrás. Positivo = acelerando."""
        vel_now = self.price_velocity(2.0)
        cutoff_2 = time.time() - 2.0
        cutoff_4 = time.time() - 4.0
        with self._lock:
            old = [t for t in self._trades if cutoff_4 <= t.timestamp < cutoff_2]
        if len(old) < 2:
            return 0.0
        elapsed = old[-1].timestamp - old[0].timestamp
        if elapsed <= 0 or old[0].price <= 0:
            return 0.0
        vel_prev = (old[-1].price - old[0].price) / old[0].price * 100.0 / elapsed
        return vel_now - vel_prev


# ═══════════════════════════════════════════════════════════════════════
# BINANCE WEBSOCKET
# [FIX 1] Callback event-driven: signal detection roda em cada trade
# ═══════════════════════════════════════════════════════════════════════

class BinanceTradeStream:
    """
    Stream btcusdt@trade da Binance.

    [FIX 1] Após cada trade, chama _hot_fn se registrada.
    _hot_fn é a detecção de sinal — roda ~1ms após o trade chegar,
    sem esperar o main loop acordar do sleep.

    Binance trade stream:
      m=False → buyer is taker → BUY pressure  (whale comprando)
      m=True  → seller is taker → SELL pressure (whale vendendo)
    """

    def __init__(self, buf: TradeBuffer, cfg: Config):
        self.buf = buf
        self.cfg = cfg
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._reconnects = 0
        self._hot_fn: Optional[Callable] = None  # [FIX 1]

    def set_hot_fn(self, fn: Callable):
        """Registra callback chamado em CADA trade na zona crítica."""
        self._hot_fn = fn

    def start(self):
        if not HAS_WEBSOCKET:
            log.warning("⚠️  websocket-client não instalado: pip install websocket-client")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="BinanceWS")
        self._thread.start()
        log.info("🔌 Binance Trade WebSocket iniciando...")

    def stop(self):
        self._running = False

    def _run(self):
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    self.cfg.binance_trade_ws,
                    on_message=self._on_message,
                    on_open=lambda ws: log.info(
                        f"✅ Binance WS conectado (reconexões: {self._reconnects})"
                    ),
                    on_error=lambda ws, e: log.debug(f"Binance WS erro: {e}"),
                    on_close=lambda ws, c, m: log.debug(f"Binance WS fechado ({c})"),
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.warning(f"Binance WS exception: {e}")
            self._reconnects += 1
            if self._running:
                time.sleep(1)

    def _on_message(self, ws, raw: str):
        try:
            d = json.loads(raw)
            trade = Trade(
                timestamp=d["T"] / 1000.0,
                price=float(d["p"]),
                quantity=float(d["q"]),
                is_taker_buy=not bool(d["m"]),
            )
            self.buf.add(trade)

            # [FIX 1] Callback imediato — sem esperar o main loop
            fn = self._hot_fn
            if fn is not None:
                fn()
        except Exception:
            pass

    def get_price_rest(self) -> float:
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
                timeout=5,
            )
            return float(r.json()["price"])
        except Exception:
            return 0.0

    def get_5m_open_price(self) -> float:
        try:
            r = requests.get(
                self.cfg.binance_kline_url,
                params={"symbol": "BTCUSDT", "interval": "5m", "limit": 1},
                timeout=5,
            )
            data = r.json()
            if data:
                price = float(data[0][1])
                log.info(f"📍 Price to Beat (5m open): ${price:,.2f}")
                return price
        except Exception as e:
            log.debug(f"Erro ao buscar 5m open: {e}")
        return 0.0


# ═══════════════════════════════════════════════════════════════════════
# POLYMARKET DATA — Mercados + Odds
# [FIX 2] Fetch UP e DOWN em paralelo (threads simultâneas)
# [FIX 3] Retorna ask prices para cache no OddsSnapshot
# ═══════════════════════════════════════════════════════════════════════

class PolymarketData:

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def interval_ts(self) -> int:
        now = int(time.time())
        return (now // 300) * 300

    def find_market(self) -> Optional[MarketInfo]:
        ts = self.interval_ts()
        for offset in [0, -300]:
            m = self._fetch_slug(f"btc-updown-5m-{ts + offset}")
            if m:
                return m
        return None

    def _fetch_slug(self, slug: str) -> Optional[MarketInfo]:
        try:
            r = requests.get(
                f"{self.cfg.gamma_host}/markets",
                params={"slug": slug, "closed": "false", "active": "true"},
                timeout=6,
            )
            data = r.json()
            if not data:
                return None
            markets = data if isinstance(data, list) else [data]
            for m in markets:
                if m.get("closed") or not m.get("active"):
                    continue
                token_ids = json.loads(m.get("clobTokenIds", "[]"))
                outcomes_raw = m.get("outcomes", "Up,Down")
                outcomes = (
                    [o.strip() for o in outcomes_raw.split(",")]
                    if isinstance(outcomes_raw, str)
                    else outcomes_raw
                )
                if len(token_ids) < 2:
                    continue
                up_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "up"), 0)
                dn_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)
                up_p, dn_p = 0.5, 0.5
                try:
                    op = m.get("outcomePrices", "")
                    if op:
                        pl = json.loads(op) if isinstance(op, str) else op
                        if len(pl) >= 2:
                            up_p, dn_p = float(pl[up_idx]), float(pl[dn_idx])
                except Exception:
                    pass
                end_ts = 0
                if m.get("endDate"):
                    try:
                        dt = datetime.fromisoformat(m["endDate"].replace("Z", "+00:00"))
                        end_ts = int(dt.timestamp())
                    except Exception:
                        pass
                return MarketInfo(
                    market_id=str(m.get("id", "")),
                    slug=slug,
                    question=m.get("question", ""),
                    end_timestamp=end_ts,
                    up_token_id=token_ids[up_idx],
                    down_token_id=token_ids[dn_idx],
                    gamma_up=up_p,
                    gamma_down=dn_p,
                )
        except Exception as e:
            log.debug(f"Erro buscando mercado {slug}: {e}")
        return None

    def fetch_odds_snapshot(self, market: MarketInfo) -> OddsSnapshot:
        """
        [FIX 2] Busca book de UP e DOWN em PARALELO (2 threads).
        Reduz o tempo de ~400ms (sequencial) para ~200ms (paralelo).
        [FIX 3] Retorna ask prices no OddsSnapshot para o executor usar.
        """
        now = time.time()

        # Resultados das threads
        up_result: dict = {}
        dn_result: dict = {}

        def fetch_up():
            try:
                up_result["data"] = self._book(market.up_token_id)
            except Exception:
                up_result["data"] = self._empty_book()

        def fetch_dn():
            try:
                dn_result["data"] = self._book(market.down_token_id)
            except Exception:
                dn_result["data"] = self._empty_book()

        # [FIX 2] Lança as 2 threads em paralelo
        t1 = threading.Thread(target=fetch_up, daemon=True)
        t2 = threading.Thread(target=fetch_dn, daemon=True)
        t1.start()
        t2.start()
        t1.join(timeout=3.0)
        t2.join(timeout=3.0)

        up_b = up_result.get("data", self._empty_book())
        dn_b = dn_result.get("data", self._empty_book())

        if up_b["price"] > 0 or dn_b["price"] > 0:
            up, dn = self._norm(up_b["price"], dn_b["price"])
            spread = max(up_b["spread"], dn_b["spread"])
            total_liq = up_b["liq"] + dn_b["liq"]
            imb = (up_b["liq"] - dn_b["liq"]) / total_liq if total_liq > 0 else 0.0
            return OddsSnapshot(
                up=up, down=dn,
                spread=spread,
                up_liquidity=up_b["liq"],
                down_liquidity=dn_b["liq"],
                book_imbalance=imb,
                up_ask=up_b["ask"] if up_b["ask"] > 0 else 0.95,    # [FIX 3]
                down_ask=dn_b["ask"] if dn_b["ask"] > 0 else 0.95,  # [FIX 3]
                source="CLOB_BOOK",
                timestamp=now,
            )

        # Fallback: Gamma cache
        return OddsSnapshot(
            up=market.gamma_up,
            down=market.gamma_down,
            up_ask=0.95,
            down_ask=0.95,
            source="GAMMA_CACHE",
            timestamp=now,
        )

    def _book(self, token_id: str) -> dict:
        r = requests.get(
            f"{self.cfg.clob_host}/book",
            params={"token_id": token_id},
            timeout=3,
        ).json()
        asks = r.get("asks", [])
        bids = r.get("bids", [])
        # CLOB pode retornar asks em ordem decrescente — pegar MELHOR (menor ask, maior bid)
        ask_p = min(float(a["price"]) for a in asks) if asks else 0.0
        bid_p = max(float(b["price"]) for b in bids) if bids else 0.0
        best_ask = min(asks, key=lambda a: float(a["price"])) if asks else None
        best_bid = max(bids, key=lambda b: float(b["price"])) if bids else None
        ask_s = float(best_ask.get("size", 0)) if best_ask else 0.0
        bid_s = float(best_bid.get("size", 0)) if best_bid else 0.0
        price = ask_p or bid_p or float(r.get("mid") or 0)
        spread = max(ask_p - bid_p, 0.0) if ask_p > 0 and bid_p > 0 else 0.0
        return {"price": price, "spread": spread, "liq": ask_s + bid_s, "ask": ask_p}

    @staticmethod
    def _empty_book() -> dict:
        return {"price": 0.0, "spread": 0.0, "liq": 0.0, "ask": 0.0}

    @staticmethod
    def _norm(up: float, dn: float) -> tuple:
        total = up + dn
        return (up / total, dn / total) if total > 0 else (up, dn)

    def check_result(self, slug: str) -> Optional[tuple]:
        try:
            r = requests.get(
                f"{self.cfg.gamma_host}/markets",
                params={"slug": slug},
                timeout=10,
            )
            data = r.json()
            if not data:
                return None
            m = data[0] if isinstance(data, list) else data

            # ── Determinar índice correto UP/DOWN (não assumir ordem fixa) ──────
            # A API pode retornar ["Up","Down"] ou ["Down","Up"] — nunca assumir.
            # Mesmo bug que causava bot=LOSS conta=WIN quando a ordem vinha invertida.
            outcomes_raw = m.get("outcomes", '["Up","Down"]')
            try:
                outcomes = (
                    json.loads(outcomes_raw)
                    if isinstance(outcomes_raw, str)
                    else list(outcomes_raw)
                )
            except Exception:
                outcomes = ["Up", "Down"]
            up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"),  0)
            dn_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "down"), 1)

            op_raw = m.get("outcomePrices", "[0.5,0.5]")
            op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
            if len(op) < 2:
                return None

            up_f = float(op[up_idx])
            dn_f = float(op[dn_idx])

            log.debug(
                f"  check_result {slug} │ resolved={m.get('resolved')} │ "
                f"up={up_f:.3f} dn={dn_f:.3f}"
            )

            # Mercado precisa estar resolvido E ter preço definitivo
            if m.get("resolved") is False:
                return None
            if up_f >= 0.85:
                return "UP", up_f, dn_f
            if dn_f >= 0.85:
                return "DOWN", up_f, dn_f
        except Exception as e:
            log.debug(f"Erro checando resultado {slug}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# ODDS CACHE — Polling background, leitura zero-latência
# [FIX 2+3] Odds paralelas + ask prices expostos
# ═══════════════════════════════════════════════════════════════════════

class OddsCache:
    """
    Thread de background que mantém odds frescas.
    Hot path lê sempre da memória — zero latência de rede.

    Intervalos:
      T <= 35s → fast mode: atualiza a cada 300ms (paralelo = ~200ms real)
      T > 35s  → slow mode: atualiza a cada 2s
    """

    def __init__(self, poly: PolymarketData, cfg: Config):
        self.poly = poly
        self.cfg = cfg
        self._snap = OddsSnapshot(up=0.5, down=0.5, up_ask=0.95, down_ask=0.95,
                                   source="INIT", timestamp=0.0)
        self._market: Optional[MarketInfo] = None
        self._lock = threading.Lock()
        self._running = False
        self._fast = False

    def set_market(self, market: Optional[MarketInfo]):
        with self._lock:
            self._market = market

    def set_fast(self, fast: bool):
        self._fast = fast

    def get(self) -> OddsSnapshot:
        with self._lock:
            return self._snap

    def get_ask(self, direction: str) -> float:
        """[FIX 3] Retorna ask price cacheado para o executor usar sem REST."""
        with self._lock:
            return self._snap.up_ask if direction == "UP" else self._snap.down_ask

    def start(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True, name="OddsCache").start()

    def stop(self):
        self._running = False

    def _run(self):
        while self._running:
            with self._lock:
                market = self._market
            if market:
                try:
                    snap = self.poly.fetch_odds_snapshot(market)
                    with self._lock:
                        self._snap = snap
                except Exception as e:
                    log.debug(f"OddsCache erro: {e}")
            # [FIX 2] Em fast mode, poll a cada 300ms (threads paralelas = ~200ms real)
            time.sleep(0.30 if self._fast else 2.0)


# ═══════════════════════════════════════════════════════════════════════
# REVERSAL DETECTOR
# ═══════════════════════════════════════════════════════════════════════

class ReversalDetector:
    """
    Detecta reversões de último segundo via pressão de volume na Binance.

    Sinal UP: BTC abaixo do target + whale comprando (>70% taker buy) +
              volume acima do normal + preço já subindo
    Sinal DOWN: simétrico
    """

    def __init__(self, cfg: Config, buf: TradeBuffer, odds: OddsCache):
        self.cfg = cfg
        self.buf = buf
        self.odds = odds
        # ── Janela de confirmação ─────────────────────────────────────────
        self._pending_dir: Optional[str] = None  # direção pendente de confirmação
        self._pending_ts: float = 0.0            # timestamp do primeiro sinal desta direção

    def reset(self):
        """Limpa estado de confirmação pendente ao trocar de mercado."""
        self._pending_dir = None
        self._pending_ts = 0.0

    def detect(self, price_to_beat: float, tr: int) -> Optional[Signal]:
        cfg = self.cfg

        if tr > cfg.reversal_start or tr < cfg.reversal_end:
            return None
        if not self.buf.is_data_fresh(max_age=2.0):
            return None

        current = self.buf.latest_price()
        if current <= 0 or price_to_beat <= 0:
            return None

        delta_pct = (current - price_to_beat) / price_to_beat * 100.0
        abs_delta = abs(delta_pct)

        buy_ratio, total_vol_3s = self.buf.buy_pressure(3.0)
        vol_surge = self.buf.vol_surge_ratio(short_s=3.0, baseline_s=20.0)
        price_vel = self.buf.price_velocity(3.0)
        accel = self.buf.price_acceleration()

        # Volume mínimo para sinal válido (~0.02 BTC em 3s)
        if total_vol_3s < 0.02:
            return None

        # ── Trade rate: confirma atividade real de whale (não um único trade gordo) ──
        trade_rate = self.buf.trade_rate(3.0)
        if trade_rate < cfg.min_trade_rate:
            log.debug(f"  REVERSAL skip: trade_rate={trade_rate:.1f}/s < {cfg.min_trade_rate}/s")
            return None

        # ── Filtro combo δ × volume ──────────────────────────────────────────
        if abs_delta >= cfg.weak_combo_delta and vol_surge < cfg.weak_combo_surge:
            log.debug(
                f"  REVERSAL skip: combo fraco "
                f"δ={abs_delta:.3f}% (≥{cfg.weak_combo_delta}%) "
                f"surge={vol_surge:.1f}x (<{cfg.weak_combo_surge}x)"
            )
            return None

        direction = self._direction(delta_pct, buy_ratio, vol_surge, price_vel, abs_delta)
        if direction is None:
            return None

        confidence = self._confidence(buy_ratio, vol_surge, price_vel, abs_delta, accel, direction)
        if confidence < 0.35:
            return None

        # ── Odds: já precificadas? (único threshold configurável) ────────────
        snap = self.odds.get()
        entry_odd = snap.up if direction == "UP" else snap.down
        if entry_odd > cfg.max_late_odd:
            log.debug(f"  REVERSAL skip: odds já precificadas ({entry_odd:.0%} > {cfg.max_late_odd:.0%})")
            return None
        if entry_odd > cfg.max_odd:
            return None

        # ── Book imbalance: crowd no CLOB concorda com a direção? ────────────
        if cfg.use_book_imbalance:
            imb = snap.book_imbalance  # +1 = toda liquidez em UP, -1 = toda em DOWN
            if direction == "UP" and imb < -cfg.max_book_imbalance_against:
                log.debug(f"  REVERSAL skip: book imbalance contra UP (imb={imb:+.2f})")
                return None
            if direction == "DOWN" and imb > cfg.max_book_imbalance_against:
                log.debug(f"  REVERSAL skip: book imbalance contra DOWN (imb={imb:+.2f})")
                return None

        # ── Janela de confirmação: sinal deve persistir N ms ─────────────────
        # A T <= 3s o tempo é escasso — usa janela reduzida para não perder a entrada
        confirm_ms = cfg.signal_confirm_ms if tr >= 4 else cfg.signal_confirm_ms_urgent
        now = time.time()
        if direction != self._pending_dir:
            # Nova direção detectada — começa a contar
            self._pending_dir = direction
            self._pending_ts = now
            log.debug(f"  REVERSAL pending: {direction} aguardando {confirm_ms:.0f}ms (T-{tr}s)")
            return None
        elapsed_ms = (now - self._pending_ts) * 1000.0
        if elapsed_ms < confirm_ms:
            log.debug(f"  REVERSAL pending: {direction} ({elapsed_ms:.0f}/{confirm_ms:.0f}ms)")
            return None
        # Sinal confirmado — limpa pending e continua para a aposta
        self._pending_dir = None
        self._pending_ts = 0.0

        # ── Bet size escalonado com confiança ─────────────────────────────────
        # confidence=0.0 → min_bet | confidence=1.0 → max_bet
        bet = cfg.min_bet + (cfg.max_bet - cfg.min_bet) * confidence

        log.info(
            f"⚡ REVERSAL {direction} │ δ={delta_pct:+.4f}% │ "
            f"buy={buy_ratio:.0%} │ surge={vol_surge:.1f}x │ rate={trade_rate:.1f}/s │ "
            f"vel={price_vel:+.5f}%/s │ acc={accel:+.5f} │ imb={snap.book_imbalance:+.2f} │ "
            f"conf={confidence:.0%} │ odd={entry_odd:.0%} │ T-{tr}s"
        )

        return Signal(
            direction=direction,
            confidence=confidence,
            source="REVERSAL",
            delta_pct=delta_pct,
            buy_pressure=buy_ratio,
            vol_surge=vol_surge,
            price_vel=price_vel,
            time_remaining=tr,
            bet_size=round(bet, 2),
            entry_odd=entry_odd,
        )

    def _direction(self, delta_pct, buy_ratio, vol_surge, price_vel, abs_delta) -> Optional[str]:
        cfg = self.cfg
        if abs_delta > cfg.max_reversal_delta:
            return None

        if (delta_pct < 0
                and buy_ratio >= cfg.buy_pressure_thresh
                and vol_surge >= cfg.min_vol_surge
                and price_vel >= cfg.min_price_vel):
            return "UP"

        if (delta_pct > 0
                and (1.0 - buy_ratio) >= cfg.buy_pressure_thresh
                and vol_surge >= cfg.min_vol_surge
                and price_vel <= -cfg.min_price_vel):
            return "DOWN"

        return None

    def _confidence(self, buy_ratio, vol_surge, price_vel, abs_delta, accel, direction) -> float:
        cfg = self.cfg
        eff = buy_ratio if direction == "UP" else (1.0 - buy_ratio)
        p_score = min((eff - cfg.buy_pressure_thresh) / (1.0 - cfg.buy_pressure_thresh), 1.0)
        s_score = min((vol_surge - 1.0) / 3.0, 1.0)
        v_score = min(abs(price_vel) / 0.005, 1.0)
        d_score = max(0.0, 1.0 - abs_delta / cfg.max_reversal_delta)
        # Aceleração na direção correta → bônus até +0.20 (era +0.10, threshold menor)
        accel_bonus = min(abs(accel) / 0.002, 0.20) if (
            (direction == "UP" and accel > 0) or (direction == "DOWN" and accel < 0)
        ) else 0.0
        return min(p_score * 0.45 + s_score * 0.30 + v_score * 0.15 + d_score * 0.10 + accel_bonus, 1.0)


# ═══════════════════════════════════════════════════════════════════════
# MOMENTUM DETECTOR — Fallback
# ═══════════════════════════════════════════════════════════════════════

class MomentumDetector:
    """Fallback: aposta na direção quando delta já é grande e claro."""

    def __init__(self, cfg: Config, buf: TradeBuffer, odds: OddsCache):
        self.cfg = cfg
        self.buf = buf
        self.odds = odds
        self._fired = False

    def reset(self):
        self._fired = False

    def detect(self, price_to_beat: float, tr: int) -> Optional[Signal]:
        cfg = self.cfg
        if self._fired or tr > cfg.momentum_start or tr < cfg.momentum_end:
            return None

        current = self.buf.latest_price()
        if current <= 0 or price_to_beat <= 0:
            return None

        delta_pct = (current - price_to_beat) / price_to_beat * 100.0
        if abs(delta_pct) < cfg.min_momentum_delta:
            return None

        direction = "UP" if delta_pct > 0 else "DOWN"

        buy_ratio, _ = self.buf.buy_pressure(10.0)
        # Threshold mais estrito (era 0.52/0.48) — exige convicção real
        if direction == "UP" and buy_ratio < cfg.momentum_buy_thresh:
            return None
        if direction == "DOWN" and buy_ratio > (1.0 - cfg.momentum_buy_thresh):
            return None

        # Confirmar com volume mínimo — momentum sem surge é apenas drift
        vol_surge_m = self.buf.vol_surge_ratio(5.0, 20.0)
        if vol_surge_m < cfg.min_momentum_vol_surge:
            return None

        snap = self.odds.get()
        entry_odd = snap.up if direction == "UP" else snap.down
        if direction == "UP" and snap.up < 0.55:
            return None
        if direction == "DOWN" and snap.down < 0.55:
            return None
        if entry_odd > cfg.max_odd:
            return None

        confidence = min(abs(delta_pct) / 0.10, 0.70)
        log.info(
            f"📈 MOMENTUM {direction} │ δ={delta_pct:+.4f}% │ "
            f"buy={buy_ratio:.0%} │ surge={vol_surge_m:.1f}x │ odd={entry_odd:.0%} │ T-{tr}s"
        )
        self._fired = True

        return Signal(
            direction=direction,
            confidence=confidence,
            source="MOMENTUM",
            delta_pct=delta_pct,
            buy_pressure=buy_ratio,
            vol_surge=vol_surge_m,
            price_vel=self.buf.price_velocity(10.0),
            time_remaining=tr,
            bet_size=cfg.min_bet,
            entry_odd=entry_odd,
        )


# ═══════════════════════════════════════════════════════════════════════
# ORDER EXECUTOR
# [FIX 3] Aceita cached_price → elimina REST call na execução
# ═══════════════════════════════════════════════════════════════════════

class OrderExecutor:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.client = None
        if not cfg.dry_run:
            self._init_client()

    def _init_client(self):
        if not self.cfg.private_key:
            log.error("❌ POLYMARKET_PRIVATE_KEY não configurada no .env!")
            return
        try:
            from py_clob_client.client import ClobClient
            self.client = ClobClient(
                host=self.cfg.clob_host,
                key=self.cfg.private_key,
                chain_id=self.cfg.chain_id,
                signature_type=self.cfg.signature_type,
                funder=self.cfg.funder_address,
            )
            self.client.set_api_creds(self.client.create_or_derive_api_creds())
            log.info("✅ CLOB client autenticado")
        except Exception as e:
            log.error(f"❌ Erro CLOB: {e}")

    def execute(self, token_id: str, amount: float, side: str, cached_price: float = 0.0) -> bool:
        """
        [FIX 3] cached_price: ask price já buscado pelo OddsCache.
        Se fornecido, elimina o REST call de /book que antes adicionava ~200ms.
        """
        amount = max(amount, self.cfg.min_bet)

        if self.cfg.dry_run:
            price_str = f"${cached_price:.4f} (cache)" if cached_price > 0 else "price=fallback"
            # Simula fill para DRY RUN mostrar valores coerentes
            sim_price = round(max(0.01, min(cached_price if cached_price > 0.01 else 0.95, 0.99)), 2)
            sim_size  = round(max(amount / sim_price, 5.0), 2)
            log.info(f"🏃 [DRY RUN] BUY {side} | ${amount:.2f} | {price_str}")
            return {
                "success": True,
                "order_id": "DRY_RUN",
                "planned_price": sim_price,
                "planned_size":  sim_size,
                "planned_cost":  round(sim_size * sim_price, 4),
            }

        if not self.client:
            log.error("❌ CLOB client não inicializado")
            return {"success": False}

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            # [FIX 3] Usar preço cacheado se disponível, fallback para REST só se necessário
            if cached_price > 0.01:
                price = cached_price
                log.info(f"⚡ Usando ask price cacheado: ${price:.4f} (sem REST)")
            else:
                # Fallback: buscar do book (só acontece se cache falhar)
                price = 0.95
                try:
                    book = requests.get(
                        f"{self.cfg.clob_host}/book",
                        params={"token_id": token_id},
                        timeout=2,
                    ).json()
                    asks = book.get("asks", [])
                    if asks:
                        price = min(float(a["price"]) for a in asks)
                    log.info(f"📡 Ask price via REST (fallback): ${price:.4f}")
                except Exception:
                    log.warning("⚠️  REST fallback também falhou, usando price=0.95")

            # CRÍTICO: preço deve ser múltiplo de 0.01 — CLOB rejeita caso contrário
            price = round(price, 2)
            price = max(0.01, min(price, 0.99))  # clamp [0.01, 0.99]

            size = round(max(amount / price, 5.0), 2)
            log.info(f"📝 Order: {size} shares @ ${price:.2f} ≈ ${size * price:.2f} USDC")

            order = self.client.create_order(
                OrderArgs(token_id=token_id, price=price, size=size, side=BUY)
            )
            resp = self.client.post_order(order, OrderType.GTC)

            # Extrai order_id da resposta (campo pode variar por versão da lib)
            order_id = ""
            if isinstance(resp, dict):
                order_id = (resp.get("orderID") or resp.get("order_id")
                            or resp.get("id") or "")
            elif hasattr(resp, "orderID"):
                order_id = resp.orderID

            log.info(f"✅ Ordem enviada │ order_id={order_id or 'N/A'} │ resp={resp}")
            return {
                "success":       True,
                "order_id":      order_id,
                "planned_price": price,
                "planned_size":  size,
                "planned_cost":  round(size * price, 4),
            }

        except Exception as e:
            log.error(f"❌ Ordem falhou: {e}")
            return {"success": False}


# ═══════════════════════════════════════════════════════════════════════
# RISK MANAGER
# ═══════════════════════════════════════════════════════════════════════

class RiskManager:
    STATS_FILE = "stats_v3.json"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.bets: list = []
        self.daily_pnl: float = 0.0
        self.total_wins: int = 0
        self.total_losses: int = 0
        self.consecutive_losses: int = 0       # global (REVERSAL)
        self.consecutive_momentum_losses: int = 0  # separado — MOMENTUM não bloqueia REVERSAL
        self.cooldown: int = 0                 # bloqueio global (REVERSAL losses)
        self.momentum_cooldown: int = 0        # bloqueio só de MOMENTUM
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.STATS_FILE):
                with open(self.STATS_FILE) as f:
                    d = json.load(f)
                self.total_wins = d.get("wins", 0)
                self.total_losses = d.get("losses", 0)
                self.daily_pnl = d.get("pnl", 0.0)
                log.info(
                    f"📊 Stats: {self.total_wins}W / {self.total_losses}L │ PnL: ${self.daily_pnl:+.2f}"
                )
        except Exception:
            pass

    def _save(self):
        try:
            with open(self.STATS_FILE, "w") as f:
                json.dump({
                    "wins": self.total_wins,
                    "losses": self.total_losses,
                    "pnl": self.daily_pnl,
                    "updated": datetime.now(timezone.utc).isoformat(),
                    "recent": [
                        {
                            "slug": b.market_slug, "side": b.side,
                            "amount": b.amount, "result": b.result,
                            "profit": b.profit, "source": b.source,
                            "buy_pres": round(b.buy_pressure, 3),
                            "vol_surge": round(b.vol_surge, 2),
                            "delta": round(b.delta_pct, 4),
                            "conf": round(b.confidence, 3),
                            "tr": b.time_remaining,
                            "odd": round(b.entry_odd, 4),
                        }
                        for b in self.bets[-100:]
                    ],
                }, f, indent=2)
        except Exception:
            pass

    def _save_async(self):
        """Salva em thread daemon — não bloqueia o hot path de aposta."""
        threading.Thread(target=self._save, daemon=True, name="RiskSave").start()

    def can_bet(self, source: str = "") -> tuple:
        """Verifica se pode apostar. source='MOMENTUM' aplica cooldown exclusivo de momentum."""
        with self._lock:
            # Cooldown global (REVERSAL losses) bloqueia tudo
            if self.cooldown > 0:
                self.cooldown -= 1
                return False, f"Cooldown global: {self.cooldown + 1} mercados restantes"
            if self.daily_pnl <= -self.cfg.max_daily_loss:
                return False, f"Stop loss diário: ${self.daily_pnl:.2f}"
            if self.consecutive_losses >= self.cfg.max_consecutive_losses:
                return False, f"{self.consecutive_losses} losses consecutivos (REVERSAL)"
            # Cooldown de momentum só bloqueia MOMENTUM — REVERSAL continua livre
            if source == "MOMENTUM" and self.momentum_cooldown > 0:
                self.momentum_cooldown -= 1
                return False, f"Momentum cooldown: {self.momentum_cooldown + 1} mercados restantes"
            return True, "OK"

    def record(self, bet: BetRecord):
        with self._lock:
            self.bets.append(bet)
        self._save_async()  # async — sem bloquear hot path

    def resolve(self, result: str, profit: float, source: str = ""):
        with self._lock:
            if not self.bets:
                return
            self.bets[-1].result = result
            self.bets[-1].profit = profit
            self.daily_pnl += profit
            if result == "WIN":
                self.total_wins += 1
                self.consecutive_losses = 0
                self.consecutive_momentum_losses = 0
            else:
                self.total_losses += 1
                if source == "MOMENTUM":
                    # Losses de MOMENTUM só afetam o cooldown de MOMENTUM
                    self.consecutive_momentum_losses += 1
                    if self.consecutive_momentum_losses >= self.cfg.max_consecutive_momentum_losses:
                        self.momentum_cooldown = self.cfg.momentum_cooldown_markets
                        self.consecutive_momentum_losses = 0
                        log.warning(f"⏸️  Momentum suspenso: {self.momentum_cooldown} mercados")
                else:
                    # Losses de REVERSAL afetam o cooldown global
                    self.consecutive_losses += 1
                    if self.consecutive_losses >= 3:
                        self.cooldown = self.cfg.cooldown_markets
                        log.warning(f"⏸️  Cooldown global: pulando {self.cooldown} mercados")
        self._save_async()  # async — sem bloquear thread de resultado

        total = self.total_wins + self.total_losses
        wr = self.total_wins / total * 100 if total > 0 else 0
        icon = "🟢" if result == "WIN" else "🔴"
        log.info(f"{icon} {result} │ ${profit:+.2f} │ PnL: ${self.daily_pnl:+.2f} │ WR: {wr:.0f}% ({self.total_wins}/{total})")

    def get_last_bet(self) -> Optional[BetRecord]:
        with self._lock:
            return self.bets[-1] if self.bets else None

    def print_result(self, bet: BetRecord, result: str, profit: float):
        total = self.total_wins + self.total_losses
        wr = self.total_wins / total * 100 if total > 0 else 0
        icon = "🟢" if result == "WIN" else "🔴"

        # Linha de fill real ou estimativa
        if bet.actual_size > 0 and bet.actual_price > 0:
            payout = bet.actual_size if result == "WIN" else 0.0
            fill_line = (
                f"  📋  Fill real: {bet.actual_size:.4f} shares @ ${bet.actual_price:.4f}  "
                f"│  Custo: ${bet.actual_cost:.4f}  │  Retorno: ${payout:.4f}\n"
            )
        else:
            fill_line = f"  📋  Fill estimado (order_id ausente ou DRY RUN)\n"

        print(
            f"\n{'═'*62}\n"
            f"  {icon}  {result}!  {bet.side} @ {bet.entry_odd:.0%}  [{bet.source}]\n"
            + fill_line +
            f"  {'💰' if result == 'WIN' else '💸'}  P&L aposta: ${profit:+.4f}  │  PnL Total: ${self.daily_pnl:+.2f}\n"
            f"  📊  {self.total_wins}W / {self.total_losses}L  │  WR: {wr:.0f}%\n"
            f"  🔬  buy={bet.buy_pressure:.0%} | surge={bet.vol_surge:.1f}x | "
            f"vel={bet.price_vel:+.5f}%/s | δ={bet.delta_pct:+.4f}%\n"
            f"{'═'*62}\n"
        )


# ═══════════════════════════════════════════════════════════════════════
# TELEMETRY
# ═══════════════════════════════════════════════════════════════════════

class Telemetry:
    def __init__(self, path: str = "telemetry_v3.jsonl"):
        self.path = path

    def log(self, event: str, data: dict):
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "event": event,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    **data,
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
# ADAPTIVE LEARNER — Aprende quais condições geram melhor resultado
# ═══════════════════════════════════════════════════════════════════════

class AdaptiveLearner:
    """
    Monitora 9 variáveis de cada aposta e aprende automaticamente
    quais faixas geram melhor win rate. Ajusta bet size dinamicamente.

    Variáveis monitoradas:
      1. time_remaining  → janela de entrada mais lucrativa (T-9s a T-2s)
      2. vol_surge       → intensidade de volume ideal (1.8x a 5x+)
      3. buy_pressure    → pressão direcional ideal (70% a 100%)
      4. delta_pct       → distância do target ideal (0% a 0.20%)
      5. price_vel       → velocidade de preço ideal (%/s)
      6. entry_odd       → faixa de odds mais lucrativa
      7. confidence      → threshold de confiança ideal
      8. direction       → UP ganha mais ou DOWN?
      9. hour_utc        → qual hora do dia tem mais edge?

    Bet sizing (composite score):
      composite = média ponderada dos multiplicadores por variável
      0.0x → skip (bucket provadamente ruim com dados suficientes)
      0.6x → abaixo da média
      1.0x → neutro (padrão)
      1.3x → bom
      1.6x → muito bom
      2.0x → excelente

    Requer MIN_SAMPLES=8 por bucket antes de influenciar decisões.
    """

    SAVE_FILE = "adaptive_learner.json"
    MIN_SAMPLES = 8  # amostras mínimas por bucket para influenciar

    # Pesos de cada variável no composite score (soma = 1.0)
    WEIGHTS = {
        "time_remaining": 0.28,
        "vol_surge":      0.20,
        "buy_pressure":   0.20,
        "delta_pct":      0.12,
        "price_vel":      0.10,
        "entry_odd":      0.05,
        "confidence":     0.03,
        "direction":      0.01,
        "hour_utc":       0.01,
    }

    # Buckets por variável: (nome, função de classificação)
    BUCKETS: dict = {
        "time_remaining": [
            ("T9-8", lambda v: v >= 8),
            ("T7-6", lambda v: 6 <= v < 8),
            ("T5-4", lambda v: 4 <= v < 6),
            ("T3-2", lambda v: 2 <= v < 4),
        ],
        "vol_surge": [
            ("surge_lo",  lambda v: 1.8 <= v < 2.5),
            ("surge_mid", lambda v: 2.5 <= v < 4.0),
            ("surge_hi",  lambda v: v >= 4.0),
        ],
        "buy_pressure": [
            ("press_lo",  lambda v: 0.70 <= v < 0.80),
            ("press_mid", lambda v: 0.80 <= v < 0.90),
            ("press_hi",  lambda v: v >= 0.90),
        ],
        "delta_pct": [
            ("delta_tiny",  lambda v: v < 0.05),
            ("delta_small", lambda v: 0.05 <= v < 0.10),
            ("delta_mid",   lambda v: v >= 0.10),
        ],
        "price_vel": [
            ("vel_lo",  lambda v: v < 0.003),
            ("vel_mid", lambda v: 0.003 <= v < 0.006),
            ("vel_hi",  lambda v: v >= 0.006),
        ],
        "entry_odd": [
            ("odd_lo",  lambda v: v < 0.55),
            ("odd_mid", lambda v: 0.55 <= v < 0.70),
            ("odd_hi",  lambda v: v >= 0.70),
        ],
        "confidence": [
            ("conf_lo",  lambda v: v < 0.50),
            ("conf_mid", lambda v: 0.50 <= v < 0.70),
            ("conf_hi",  lambda v: v >= 0.70),
        ],
        "direction": [
            ("UP",   lambda v: v == "UP"),
            ("DOWN", lambda v: v == "DOWN"),
        ],
        "hour_utc": [
            ("h00-06", lambda v: 0 <= v < 6),
            ("h06-12", lambda v: 6 <= v < 12),
            ("h12-18", lambda v: 12 <= v < 18),
            ("h18-24", lambda v: 18 <= v < 24),
        ],
    }

    def __init__(self):
        self.stats: dict = {}
        self._lock = threading.Lock()
        self._total_bets = 0
        self._init_stats()
        self._load()

    def _init_stats(self):
        for var, buckets in self.BUCKETS.items():
            self.stats[var] = {name: {"wins": 0, "losses": 0} for name, _ in buckets}

    def _load(self):
        try:
            if os.path.exists(self.SAVE_FILE):
                with open(self.SAVE_FILE) as f:
                    d = json.load(f)
                for var, buckets in d.get("stats", {}).items():
                    if var in self.stats:
                        for bucket, vals in buckets.items():
                            if bucket in self.stats[var]:
                                self.stats[var][bucket] = vals
                self._total_bets = d.get("total_bets", 0)
                log.info(f"🧠 AdaptiveLearner: {self._total_bets} apostas históricas carregadas")
        except Exception as e:
            log.debug(f"AdaptiveLearner load: {e}")

    def _save(self):
        try:
            with open(self.SAVE_FILE, "w") as f:
                json.dump({
                    "stats": self.stats,
                    "total_bets": self._total_bets,
                    "updated": datetime.now(timezone.utc).isoformat(),
                }, f, indent=2)
        except Exception:
            pass

    def _get_bucket(self, var: str, value) -> Optional[str]:
        for name, fn in self.BUCKETS.get(var, []):
            try:
                if fn(value):
                    return name
            except Exception:
                pass
        return None

    def _win_rate(self, var: str, bucket: str) -> tuple:
        """Retorna (win_rate, total_amostras)."""
        b = self.stats.get(var, {}).get(bucket, {"wins": 0, "losses": 0})
        total = b["wins"] + b["losses"]
        return (b["wins"] / total if total > 0 else 0.5), total

    def _multiplier(self, win_rate: float, total: int) -> float:
        """Converte win_rate em multiplicador de bet (0.0=skip, 2.0=máximo)."""
        if total < self.MIN_SAMPLES:
            return 1.0   # dados insuficientes → neutro, não influencia
        if win_rate < 0.38: return 0.0   # skip: provadamente ruim
        if win_rate < 0.48: return 0.6   # abaixo da média → reduz
        if win_rate < 0.55: return 1.0   # neutro
        if win_rate < 0.65: return 1.3   # bom → aumenta
        if win_rate < 0.75: return 1.6   # muito bom → aumenta mais
        return 2.0                        # excelente → dobra

    def _extract(self, bet: BetRecord) -> dict:
        """Extrai valores normalizados de um BetRecord."""
        eff = bet.buy_pressure if bet.side == "UP" else (1.0 - bet.buy_pressure)
        hour = datetime.fromtimestamp(bet.timestamp, tz=timezone.utc).hour
        return {
            "time_remaining": bet.time_remaining,
            "vol_surge":      bet.vol_surge,
            "buy_pressure":   eff,
            "delta_pct":      abs(bet.delta_pct),
            "price_vel":      abs(bet.price_vel),
            "entry_odd":      bet.entry_odd,
            "confidence":     bet.confidence,
            "direction":      bet.side,
            "hour_utc":       hour,
        }

    def _signal_values(self, signal: Signal) -> dict:
        """Extrai valores de um Signal (pré-aposta)."""
        eff = signal.buy_pressure if signal.direction == "UP" else (1.0 - signal.buy_pressure)
        return {
            "time_remaining": signal.time_remaining,
            "vol_surge":      signal.vol_surge,
            "buy_pressure":   eff,
            "delta_pct":      abs(signal.delta_pct),
            "price_vel":      abs(signal.price_vel),
            "entry_odd":      signal.entry_odd,
            "confidence":     signal.confidence,
            "direction":      signal.direction,
            "hour_utc":       datetime.now(timezone.utc).hour,
        }

    def evaluate(self, signal: Signal) -> tuple:
        """
        Avalia sinal e retorna (composite_mult, should_bet, info_dict).

        composite_mult: 0.0 (skip) → 2.0 (excelente)
        should_bet: False se variável importante tem edge negativo provado
        info_dict: detalhes por variável para logging
        """
        values = self._signal_values(signal)
        composite_sum = 0.0
        weight_sum = 0.0
        hard_skip = False
        info = {}

        for var, weight in self.WEIGHTS.items():
            val = values.get(var)
            if val is None:
                continue
            bucket = self._get_bucket(var, val)
            if bucket is None:
                continue
            wr, total = self._win_rate(var, bucket)
            mult = self._multiplier(wr, total)
            info[var] = {"b": bucket, "wr": round(wr, 3), "n": total, "m": round(mult, 2)}

            # Hard skip: variável importante com edge negativo confirmado
            if mult == 0.0 and weight >= 0.10 and total >= self.MIN_SAMPLES:
                hard_skip = True

            composite_sum += mult * weight
            weight_sum += weight

        composite = composite_sum / weight_sum if weight_sum > 0 else 1.0

        # Skip por composite baixo (com dados suficientes)
        if self._total_bets >= 30 and composite < 0.50:
            hard_skip = True

        return composite, not hard_skip, info

    def sized_bet(self, signal: Signal, base: float, mn: float, mx: float) -> tuple:
        """
        Retorna (bet_amount, should_bet, composite_mult).
        bet_amount ajustado entre min_bet e max_bet pelo composite.
        """
        composite, should_bet, info = self.evaluate(signal)
        if not should_bet:
            # Log quais variáveis causaram o skip
            skipped = [f"{v}={d['b']}({d['wr']:.0%})" for v, d in info.items() if d["m"] == 0.0]
            log.info(f"🧠 Learner SKIP │ composite={composite:.2f} │ {', '.join(skipped) or 'low composite'}")
            return 0.0, False, composite
        amount = max(mn, min(base * composite, mx))
        return round(amount, 2), True, composite

    def record(self, bet: BetRecord, won: bool):
        """Registra resultado de uma aposta em todos os buckets relevantes."""
        values = self._extract(bet)
        with self._lock:
            for var, val in values.items():
                bucket = self._get_bucket(var, val)
                if bucket and var in self.stats and bucket in self.stats[var]:
                    self.stats[var][bucket]["wins" if won else "losses"] += 1
            self._total_bets += 1
            self._save()
        log.info(f"🧠 Learner │ {'WIN ✅' if won else 'LOSS ❌'} registrado │ total={self._total_bets}")

    def best_windows(self) -> list:
        """Retorna os buckets com melhor performance (para log rápido)."""
        results = []
        for var in ["time_remaining", "vol_surge", "buy_pressure"]:
            best_name, best_wr, best_n = None, 0.0, 0
            for name, _ in self.BUCKETS.get(var, []):
                wr, total = self._win_rate(var, name)
                if total >= self.MIN_SAMPLES and wr > best_wr:
                    best_wr, best_name, best_n = wr, name, total
            if best_name:
                results.append(f"{var}={best_name}({best_wr:.0%},n={best_n})")
        return results

    def print_report(self):
        """Imprime relatório completo de aprendizado."""
        if self._total_bets < 3:
            print("🧠 AdaptiveLearner: aguardando dados (mínimo 3 apostas)")
            return

        lines = [
            f"\n{'═'*64}",
            f"  🧠  ADAPTIVE LEARNER  ─  {self._total_bets} apostas registradas",
            f"{'═'*64}",
        ]

        for var, weight in self.WEIGHTS.items():
            buckets_data = self.stats.get(var, {})
            has_data = any(b["wins"] + b["losses"] > 0 for b in buckets_data.values())
            if not has_data:
                continue
            lines.append(f"\n  [{weight:.0%}] {var}:")
            for name, _ in self.BUCKETS.get(var, []):
                b = buckets_data.get(name, {"wins": 0, "losses": 0})
                total = b["wins"] + b["losses"]
                if total == 0:
                    continue
                wr = b["wins"] / total
                mult = self._multiplier(wr, total)
                bar = "█" * int(wr * 10) + "░" * (10 - int(wr * 10))
                if mult == 0.0:
                    status = "🔴 SKIP"
                elif mult >= 1.3:
                    status = f"🟢 {mult:.1f}x"
                else:
                    status = f"🟡 {mult:.1f}x"
                lines.append(
                    f"    {name:<14} [{bar}] {wr:.0%} "
                    f"({b['wins']}W/{b['losses']}L  n={total:>3})  {status}"
                )

        best = self.best_windows()
        if best:
            lines.append(f"\n  ⭐ Melhores janelas: {' │ '.join(best)}")
        lines.append(f"{'═'*64}\n")
        print("\n".join(lines))


# ═══════════════════════════════════════════════════════════════════════
# RESULT CHECKER — Background (não bloqueia main loop)
# ═══════════════════════════════════════════════════════════════════════

class ResultChecker:
    """
    Verifica resultado de apostas usando 3 fontes em ordem de confiabilidade:

    1. CLOB Polymarket (via preço dos tokens UP/DOWN)
       Após resolução: winner → $1.00, loser → $0.00
       Esta é a fonte mais confiável — é exatamente o que a plataforma mostra.

    2. Gamma API (outcomePrices)
       API da Polymarket que retorna o resultado oficial do mercado.
       Pode demorar alguns minutos para atualizar após a resolução.

    3. [REMOVIDO] Binance fallback — era a fonte de erros!
       O preço atual do BTC não reflete o preço do oráculo no momento do close.
       Foi substituído por: espera mais longa no Gamma API.
    """

    def __init__(self, poly: PolymarketData, risk: RiskManager, buf: TradeBuffer,
                 tel: Telemetry, learner: "AdaptiveLearner",
                 executor: "OrderExecutor"):
        self.poly = poly
        self.risk = risk
        self.buf = buf
        self.tel = tel
        self.learner = learner
        self.executor = executor  # para verificar preços dos tokens diretamente

    def check_async(self, bet: BetRecord, market_end_ts: int):
        threading.Thread(
            target=self._check,
            args=(bet, market_end_ts),
            daemon=True,
            name=f"Result-{bet.market_slug[-8:]}",
        ).start()

    # ── FONTE 1: CLOB token price (mais confiável — é o que a plataforma mostra) ──

    def _clob_winner(self, bet: BetRecord) -> Optional[str]:
        """
        Verifica resultado pelos preços dos tokens diretamente no CLOB da Polymarket.

        Após resolução:
          Token vencedor → preço sobe para $1.00 (ou fica em $0.99+)
          Token perdedor → preço cai para $0.00 (ou fica em $0.01-)

        Esta é EXATAMENTE a mesma informação que aparece na sua conta.
        """
        if not self.executor.client:
            return None
        if not bet.up_token_id or not bet.down_token_id:
            return None

        try:
            results = {}
            for side, token_id in [("UP", bet.up_token_id), ("DOWN", bet.down_token_id)]:
                try:
                    r = self.executor.client.get_last_trade_price(token_id=token_id)
                    price = float(r.get("price", 0) if isinstance(r, dict) else r or 0)
                    results[side] = price
                except Exception:
                    # Tentar via midpoint se last_trade_price falhar
                    try:
                        r2 = self.executor.client.get_midpoint(token_id=token_id)
                        price = float(r2.get("mid", 0) if isinstance(r2, dict) else r2 or 0)
                        results[side] = price
                    except Exception:
                        results[side] = 0.0

            up_p = results.get("UP", 0.0)
            dn_p = results.get("DOWN", 0.0)

            log.info(
                f"  CLOB prices │ UP={up_p:.3f}  DOWN={dn_p:.3f}  "
                f"│ bet={bet.side}"
            )

            # Resolução confirmada: um lado precisa estar próximo de 1.0
            if up_p >= 0.90:
                return "UP"
            if dn_p >= 0.90:
                return "DOWN"

            return None  # ainda não resolvido
        except Exception as e:
            log.debug(f"_clob_winner: {e}")
            return None

    # ── FONTE 2: Gamma API (outcomePrices) ──────────────────────────────────────

    def _gamma_winner(self, slug: str) -> Optional[tuple]:
        """Retorna (winner, up_final, dn_final) ou None se não resolvido ainda."""
        return self.poly.check_result(slug)

    # ── ORQUESTRADOR PRINCIPAL ───────────────────────────────────────────────────

    def _check(self, bet: BetRecord, market_end_ts: int):
        # Esperar o mercado fechar + buffer de segurança
        wait = max(0, market_end_ts + 10 - time.time())
        if wait > 0:
            time.sleep(wait)
        # Se scalp já foi executado, resultado já foi registrado — não duplicar
        if bet.scalp_executed:
            log.info(f"⏭️  Scalp já executado para {bet.market_slug} — ResultChecker ignorado")
            return

        log.info(f"Verificando resultado: {bet.market_slug} (lado={bet.side})...")

        # ── Tentar por até 30 minutos com backoff progressivo ─────────────
        # Polymarket pode demorar mais do que 5min para resolver via Chainlink.
        # Backoff: 15s (primeiros 6) → 30s (próximos 6) → 60s (restante)
        max_wait = 30 * 60  # 30 minutos
        deadline = time.time() + max_wait
        attempt  = 0

        while time.time() < deadline:

            # FONTE 1: CLOB (plataforma direta) — tenta primeiro
            clob_winner = self._clob_winner(bet)
            if clob_winner:
                self._resolve(bet, clob_winner, None, None, resolver="CLOB_PRICE")
                return

            # FONTE 2: Gamma API — fallback mas ainda confiável
            gamma = self._gamma_winner(bet.market_slug)
            if gamma:
                winner, up_f, dn_f = gamma
                self._resolve(bet, winner, up_f, dn_f, resolver="GAMMA_API")
                return

            attempt += 1
            gap = 15 if attempt <= 6 else (30 if attempt <= 12 else 60)
            mins_left = int((deadline - time.time()) / 60)
            log.info(
                f"  Aguardando resolucao... tentativa {attempt} "
                f"(proxima em {gap}s │ até {mins_left}min restantes)"
            )
            time.sleep(gap)

        # ── Após 30 min sem resolução: registrar como indeterminado ──
        log.warning(
            f"Resultado indeterminado apos 30min: {bet.market_slug} "
            f"| Verifique manualmente na Polymarket."
        )
        self.tel.log("result_unknown", {
            "market": bet.market_slug, "bet_side": bet.side,
            "note": "CLOB e Gamma nao responderam em 30min",
        })

    def _fetch_real_fill(self, bet: BetRecord):
        """
        Busca dados reais do fill via CLOB get_order().
        Atualiza bet.actual_size / actual_price / actual_cost com valores reais.
        Só executa se tiver order_id válido e cliente CLOB disponível.
        """
        if not bet.order_id or bet.order_id == "DRY_RUN":
            return
        if not self.executor or not self.executor.client:
            return
        try:
            data = self.executor.client.get_order(bet.order_id)
            if not isinstance(data, dict):
                return
            size_matched = float(data.get("size_matched") or data.get("sizeFilled") or 0)
            fill_price   = float(data.get("price") or 0)
            if size_matched > 0 and fill_price > 0:
                bet.actual_size  = size_matched
                bet.actual_price = fill_price
                bet.actual_cost  = round(size_matched * fill_price, 4)
                log.info(
                    f"📋 Fill real: {size_matched:.4f} shares @ ${fill_price:.4f} "
                    f"= ${bet.actual_cost:.4f} USDC (order_id={bet.order_id[:12]}...)"
                )
            else:
                log.debug(f"get_order retornou size_matched=0 para {bet.order_id}")
        except Exception as e:
            log.debug(f"_fetch_real_fill falhou: {e}")

    def _resolve(self, bet: BetRecord, winner: str,
                 up_f: Optional[float], dn_f: Optional[float],
                 resolver: str):
        """Registra resultado, atualiza stats e learner."""
        # 🔍 Primeiro: buscar fill real na CLOB (sem impacto na latência de entrada)
        self._fetch_real_fill(bet)

        won = bet.side == winner
        result_str = "WIN" if won else "LOSS"

        # ── Calcular lucro com dados reais se disponíveis ──────────────
        if bet.actual_size > 0 and bet.actual_price > 0:
            # Cálculo exato: paga $1 por share se venceu, $0 se perdeu
            payout = bet.actual_size if won else 0.0
            profit = round(payout - bet.actual_cost, 4)
            data_source = "CLOB_FILL_REAL"
        else:
            # Fallback: estimativa via odd de entrada
            safe_odd = max(bet.entry_odd, 0.01)
            profit = (bet.amount / safe_odd - bet.amount) if won else -bet.amount
            data_source = "ESTIMADO"

        self.risk.resolve(result_str, profit, source=bet.source)
        self.learner.record(bet, won)       # 🧠 Aprendizado

        last = self.risk.get_last_bet()
        if last:
            self.risk.print_result(last, result_str, profit)

        # ── Log detalhado com valores reais ───────────────────────────
        real_info = ""
        if bet.actual_size > 0:
            payout_real = bet.actual_size if won else 0.0
            real_info = (
                f"\n  💎 Fill real  : {bet.actual_size:.4f} shares @ ${bet.actual_price:.4f}"
                f"\n  💸 Custo real : ${bet.actual_cost:.4f} USDC"
                f"\n  💰 Retorno    : ${payout_real:.4f} USDC  →  P&L = ${profit:+.4f}"
            )
        log.info(
            f"Resultado via {resolver} [{data_source}]: {result_str} "
            f"| {bet.side} vs winner={winner} | lucro=${profit:+.4f}"
            + real_info
        )

        self.tel.log("result", {
            "market":       bet.market_slug,
            "bet_side":     bet.side,
            "winner":       winner,
            "result":       result_str,
            "profit":       profit,
            "up_final":     up_f,
            "dn_final":     dn_f,
            "resolver":     resolver,
            "profit_src":   data_source,
            # Dados reais do fill
            "actual_size":  bet.actual_size,
            "actual_price": bet.actual_price,
            "actual_cost":  bet.actual_cost,
            "actual_payout": bet.actual_size if won else 0.0,
        })


# ═══════════════════════════════════════════════════════════════════════
# SCALP MANAGER — saída antecipada para garantir lucro
# ═══════════════════════════════════════════════════════════════════════

class ScalpManager:
    """
    Após uma entrada, monitora as odds em tempo real.
    Quando a posição valoriza o suficiente (odds ≥ scalp_exit_odd),
    vende os tokens antes da resolução — garantindo o lucro imediato.

    Lógica do bid price:
      Mercado binário: UP + DOWN = $1.00
      Bid(UP) ≈ 1 - Ask(DOWN)
      Exemplo: se DOWN ask = 0.08 → posso vender UP por ~$0.92

    Timeline:
      T-9s  → entrada em UP @ 0.75
      T-6s  → odds UP chegam a 0.92 → SCALP! vende @ ~0.91
      T-0   → resolução normal ignorada (já resolvido via scalp)
    """

    def __init__(self, cfg: Config, executor: "OrderExecutor",
                 odds: OddsCache, risk: RiskManager,
                 learner: "AdaptiveLearner", tel: Telemetry):
        self.cfg = cfg
        self.executor = executor
        self.odds = odds
        self.risk = risk
        self.learner = learner
        self.tel = tel

    def watch(self, bet: "BetRecord", market: "MarketInfo"):
        """Inicia thread de monitoramento pós-entrada."""
        if not self.cfg.enable_scalp:
            return
        threading.Thread(
            target=self._monitor,
            args=(bet, market),
            daemon=True,
            name=f"Scalp-{market.slug[-8:]}",
        ).start()

    def _monitor(self, bet: "BetRecord", market: "MarketInfo"):
        cfg = self.cfg
        token_id = market.up_token_id if bet.side == "UP" else market.down_token_id

        while True:
            tr = market.end_timestamp - int(time.time())
            if tr < cfg.scalp_min_time or bet.scalp_executed:
                break

            snap = self.odds.get()
            current_odd = snap.up if bet.side == "UP" else snap.down

            if current_odd < cfg.scalp_exit_odd:
                time.sleep(cfg.scalp_poll_ms / 1000.0)
                continue

            # ── Threshold atingido: calcular bid e vender ──────────────
            opp_ask = snap.down_ask if bet.side == "UP" else snap.up_ask
            bid_price = round(max(0.01, min(1.0 - opp_ask, 0.99)), 2)

            # Shares: usa actual_size (preenchido logo após execução da ordem)
            size = bet.actual_size
            if size <= 0:
                size = round(bet.amount / max(bet.entry_odd, 0.01), 2)

            profit = round(size * (bid_price - bet.entry_odd), 4)

            log.info(
                f"💰 SCALP {bet.side} │ "
                f"odds={current_odd:.0%} ≥ {cfg.scalp_exit_odd:.0%} │ "
                f"bid={bid_price:.2f} │ entry={bet.entry_odd:.2f} │ "
                f"shares={size:.2f} │ profit≈${profit:+.2f} │ T-{tr}s"
            )

            success = self._execute_sell(token_id, size, bid_price)
            if success:
                bet.scalp_executed = True
                bet.scalp_profit = profit
                self.risk.resolve("WIN", profit, source=bet.source)
                self.learner.record(bet, True)
                self.tel.log("scalp", {
                    "market": bet.market_slug,
                    "side": bet.side,
                    "entry_odd": bet.entry_odd,
                    "exit_odd": round(current_odd, 4),
                    "bid_price": bid_price,
                    "shares": size,
                    "profit": profit,
                    "time_remaining": tr,
                })
                print(
                    f"\n{'═'*62}\n"
                    f"  💰  SCALP {bet.side} @ {bid_price:.2f}  [saída antecipada]\n"
                    f"  📈  Entrada: {bet.entry_odd:.2f} → Saída: {bid_price:.2f} "
                    f"(+{bid_price - bet.entry_odd:.2f}/share)\n"
                    f"  💵  Lucro garantido: ${profit:+.4f} │ T-{tr}s restantes\n"
                    f"{'═'*62}\n"
                )
            break

    def _execute_sell(self, token_id: str, size: float, price: float) -> bool:
        if self.cfg.dry_run:
            log.info(f"🏃 [DRY RUN] SCALP SELL {size:.2f} shares @ ${price:.2f}")
            return True

        if not self.executor.client:
            log.error("❌ CLOB client não disponível para scalp sell")
            return False

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL

            order = self.executor.client.create_order(
                OrderArgs(token_id=token_id, price=price, size=size, side=SELL)
            )
            resp = self.executor.client.post_order(order, OrderType.GTC)
            log.info(f"✅ Scalp SELL enviado │ resp={resp}")
            return True
        except Exception as e:
            log.error(f"❌ Scalp SELL falhou: {e}")
            return False


# ═══════════════════════════════════════════════════════════════════════
# MAIN BOT
# [FIX 1] Event-driven via _on_trade_hot() — detecção imediata
# [FIX 4] Main loop como backup com sleep mínimo (20ms na zona)
# ═══════════════════════════════════════════════════════════════════════

class PolyBot:

    def __init__(self):
        self.cfg = Config()
        self.buf = TradeBuffer(history_seconds=60.0)
        self.binance = BinanceTradeStream(self.buf, self.cfg)
        self.poly = PolymarketData(self.cfg)
        self.odds_cache = OddsCache(self.poly, self.cfg)
        self.reversal = ReversalDetector(self.cfg, self.buf, self.odds_cache)
        self.momentum = MomentumDetector(self.cfg, self.buf, self.odds_cache)
        self.executor = OrderExecutor(self.cfg)
        self.risk = RiskManager(self.cfg)
        self.tel = Telemetry()
        self.learner = AdaptiveLearner()                                   # 🧠
        self.result_checker = ResultChecker(
            self.poly, self.risk, self.buf, self.tel,
            self.learner, self.executor                                    # executor → verifica resultado via CLOB
        )
        self.scalp = ScalpManager(
            self.cfg, self.executor, self.odds_cache,
            self.risk, self.learner, self.tel,
        )

        self.current_market: Optional[MarketInfo] = None
        self.bet_placed_for: Optional[str] = None

        # [FIX 1] Lock para evitar double-bet em chamadas simultâneas
        self._bet_lock = threading.Lock()

        self._running = True
        self._log_tick = 0

        # [THROTTLE] Controla frequência máxima do hot callback (50ms)
        self._last_hot_call: float = 0.0

        # [FIX 1] Registrar callback event-driven no WebSocket
        self.binance.set_hot_fn(self._on_trade_hot)

    # ──────────────────────────────────────────────────────────────────
    # [FIX 1] HOT CALLBACK — chamado em CADA trade pelo WebSocket thread
    # Latência: ~1ms após o trade chegar (sem sleep, sem polling)
    # ──────────────────────────────────────────────────────────────────

    def _on_trade_hot(self):
        """
        Chamado diretamente da thread do WebSocket Binance após cada aggTrade.
        Roda apenas na zona de reversão (T-9s a T-2s).
        Detecção em ~1ms — sem esperar o main loop acordar.

        [THROTTLE] 50ms mínimo entre chamadas: evita sobrecarga durante
        whale burst (quando o trade rate é mais alto e os cálculos O(n)
        seriam chamados centenas de vezes/segundo, criando gargalo).
        """
        # [THROTTLE] Ignorar calls em menos de 50ms do anterior
        now = time.time()
        if now - self._last_hot_call < 0.050:
            return
        self._last_hot_call = now

        market = self.current_market
        if not market or not market.price_to_beat:
            return

        tr = self._time_remaining(market)

        # Só ativa na janela de reversão
        if tr > self.cfg.reversal_start or tr < self.cfg.reversal_end:
            return

        # Fast check sem lock (leitura atômica em CPython)
        if self.bet_placed_for == market.slug:
            return

        signal = self.reversal.detect(market.price_to_beat, tr)
        if signal is None:
            return

        # Passar o ask price cacheado para evitar REST na execução
        snap = self.odds_cache.get()
        cached_ask = snap.up_ask if signal.direction == "UP" else snap.down_ask

        self._place_bet(market, signal, tr, cached_ask=cached_ask)

    # ──────────────────────────────────────────────────────────────────
    # MAIN LOOP — backup + gerenciamento de mercado + momentum
    # ──────────────────────────────────────────────────────────────────

    def _time_remaining(self, market: MarketInfo) -> int:
        if market.end_timestamp > 0:
            return max(0, market.end_timestamp - int(time.time()))
        return max(0, self.poly.interval_ts() + 300 - int(time.time()))

    def _run_cycle(self):
        # ── 1. Verificar mercado ──
        expected_slug = f"btc-updown-5m-{self.poly.interval_ts()}"

        if not self.current_market or self.current_market.slug != expected_slug:
            market = self.poly.find_market()
            if not market:
                return

            self.current_market = market
            self.bet_placed_for = None
            self.momentum.reset()
            self.reversal.reset()   # limpa sinal pendente de confirmação

            # ── PTB: Binance 5m open (primário) → bloqueio se falhar ──────────
            # NUNCA usar o preço atual como PTB — causaria delta=0 e decisões erradas.
            ptb_binance = self.binance.get_5m_open_price()
            if ptb_binance > 0:
                market.price_to_beat = ptb_binance
                log.info(f"✅ PTB Binance 5m: ${ptb_binance:,.4f}")
            else:
                log.error("❌ PTB indisponível (Binance falhou) — mercado bloqueado")
                market.price_to_beat = 0.0  # detect() retorna None quando ptb <= 0

            self.odds_cache.set_market(market)
            log.info(
                f"🆕 Mercado: {market.slug} │ "
                f"PTB: ${market.price_to_beat:,.4f} │ "
                f"Fecha em: {self._time_remaining(market)}s"
            )

        market = self.current_market
        tr = self._time_remaining(market)

        # ── 2. Ativar odds rápidas perto da zona ──
        self.odds_cache.set_fast(tr <= 35)

        # ── 3. Log de status ──
        self._log_status(market, tr)

        # ── 4. Já apostou → skip ──
        if self.bet_placed_for == market.slug:
            return

        # ── 5. Verificar risco ──
        ok, reason = self.risk.can_bet()  # verifica bloqueio global
        if not ok:
            if tr <= self.cfg.reversal_start + 2:
                log.warning(f"⚠️  Risk block: {reason}")
            return

        # ── 6. Reversal (backup do event-driven, janela T-9s a T-2s) ──
        # O event-driven já dispara na maioria dos casos.
        # O main loop serve como fallback e para o momentum.
        signal = None
        if tr <= self.cfg.reversal_start:
            signal = self.reversal.detect(market.price_to_beat, tr)
            if signal:
                snap = self.odds_cache.get()
                cached_ask = snap.up_ask if signal.direction == "UP" else snap.down_ask
                self._place_bet(market, signal, tr, cached_ask=cached_ask)
                return

        # ── 7. Fallback momentum (T-10s a T-5s) — só se MOMENTUM não está em cooldown ──
        if self.cfg.momentum_end <= tr <= self.cfg.momentum_start:
            ok_mom, _ = self.risk.can_bet(source="MOMENTUM")
            if ok_mom:
                signal = self.momentum.detect(market.price_to_beat, tr)
                if signal:
                    snap = self.odds_cache.get()
                    cached_ask = snap.up_ask if signal.direction == "UP" else snap.down_ask
                    self._place_bet(market, signal, tr, cached_ask=cached_ask)

    def _place_bet(self, market: MarketInfo, signal: Signal, tr: int, cached_ask: float = 0.0):
        """
        Thread-safe com _bet_lock: garante que dois threads (WS + main loop)
        não apostem ao mesmo tempo no mesmo mercado.
        Integra AdaptiveLearner para bet sizing dinâmico baseado em histórico.
        """
        with self._bet_lock:
            # Double-check dentro do lock
            if self.bet_placed_for == market.slug:
                return

            ok, reason = self.risk.can_bet(source=signal.source)
            if not ok:
                return

            # 🧠 Adaptive bet sizing — usa signal.bet_size (confiança já embutida) como base
            # confidence=0 → min_bet | confidence=1 → max_bet (fórmula linear corrigida)
            bet_amount, should_bet, composite = self.learner.sized_bet(
                signal, signal.bet_size, self.cfg.min_bet, self.cfg.max_bet
            )
            if not should_bet:
                return  # Learner identificou condição historicamente ruim → skip

            token_id = market.up_token_id if signal.direction == "UP" else market.down_token_id
            current = self.buf.latest_price()

            print(
                f"\n{'─'*60}\n"
                f"  🎯  {signal.source}: {signal.direction} │ Conf: {signal.confidence:.0%} │ T-{tr}s\n"
                f"  💵  ${bet_amount:.2f} @ odd={signal.entry_odd:.0%}  "
                f"[learner={composite:.2f}x]  {'[cached ask]' if cached_ask > 0 else '[REST ask]'}\n"
                f"  📊  buy={signal.buy_pressure:.0%} │ surge={signal.vol_surge:.1f}x │ vel={signal.price_vel:+.5f}%/s\n"
                f"  📍  δ={signal.delta_pct:+.4f}% │ BTC=${current:,.2f} │ PTB=${market.price_to_beat:,.2f}\n"
                f"{'─'*60}"
            )

            # [FIX 3] Passa cached_ask → executor não faz REST call
            exec_result = self.executor.execute(token_id, bet_amount, signal.direction, cached_price=cached_ask)

            if isinstance(exec_result, dict) and exec_result.get("success"):
                bet = BetRecord(
                    timestamp=time.time(),
                    market_slug=market.slug,
                    side=signal.direction,
                    amount=bet_amount,
                    entry_odd=signal.entry_odd,
                    delta_pct=signal.delta_pct,
                    confidence=signal.confidence,
                    time_remaining=tr,
                    source=signal.source,
                    buy_pressure=signal.buy_pressure,
                    vol_surge=signal.vol_surge,
                    price_vel=signal.price_vel,
                    price_at_entry=current,
                    price_to_beat=market.price_to_beat,
                    # Salva tokens para verificar resultado diretamente no CLOB
                    up_token_id=market.up_token_id,
                    down_token_id=market.down_token_id,
                    # Fill planejado (será confirmado pelo ResultChecker via get_order)
                    order_id=exec_result.get("order_id", ""),
                    actual_price=exec_result.get("planned_price", 0.0),
                    actual_size=exec_result.get("planned_size", 0.0),
                    actual_cost=exec_result.get("planned_cost", 0.0),
                )
                self.risk.record(bet)
                self.bet_placed_for = market.slug
                self.result_checker.check_async(bet, market.end_timestamp)
                self.scalp.watch(bet, market)  # monitora saída antecipada
                self.tel.log("entry", {
                    "market": market.slug,
                    "side": signal.direction,
                    "amount": bet_amount,
                    "entry_odd": signal.entry_odd,
                    "delta_pct": signal.delta_pct,
                    "confidence": signal.confidence,
                    "time_remaining": tr,
                    "source": signal.source,
                    "buy_pressure": signal.buy_pressure,
                    "vol_surge": signal.vol_surge,
                    "price_vel": signal.price_vel,
                    "cached_ask_used": cached_ask > 0,
                    "learner_composite": round(composite, 3),
                    "order_id": exec_result.get("order_id", ""),
                    "planned_cost": exec_result.get("planned_cost", 0.0),
                })

    def _log_status(self, market: MarketInfo, tr: int):
        current = self.buf.latest_price()
        ptb = market.price_to_beat
        delta = (current - ptb) / ptb * 100.0 if ptb > 0 else 0.0
        snap = self.odds_cache.get()
        dom = "UP" if snap.up > snap.down else "DOWN"
        dom_pct = max(snap.up, snap.down)

        extra = ""
        if tr <= self.cfg.reversal_start + 3:
            bp, vol_3 = self.buf.buy_pressure(3.0)
            surge = self.buf.vol_surge_ratio(3.0, 20.0)
            vel = self.buf.price_velocity(3.0)
            fresh = "✅" if self.buf.is_data_fresh() else "❌WS"
            ask_up = snap.up_ask
            extra = (
                f" │ buy={bp:.0%} surge={surge:.1f}x vel={vel:+.5f}%/s "
                f"vol3s={vol_3:.3f}BTC ask={ask_up:.3f} {fresh}"
            )

        if tr > 30:
            self._log_tick += 1
            if self._log_tick % 5 != 0:
                return

        zone = ""
        if tr <= self.cfg.reversal_start:
            zone = " ⚡ REVERSAL ZONE!"
        elif tr <= self.cfg.momentum_start:
            zone = " 📈 scanning..."

        log.info(
            f"📊 {market.slug[-15:]} │ {dom} {dom_pct:.0%} │ "
            f"${current:,.0f} ({delta:+.4f}%) │ {tr}s{zone}{extra}"
        )

    def _banner(self):
        mode = "🏃 DRY RUN" if self.cfg.dry_run else "💰 LIVE"
        ws = "✅ websocket-client" if HAS_WEBSOCKET else "❌ FALTANDO"
        print(f"""
╔═══════════════════════════════════════════════════════════╗
║  ₿  POLYMARKET LAST-SECOND REVERSAL BOT v3.1             ║
╠═══════════════════════════════════════════════════════════╣
║  [FIX 1] Event-driven: detecção em cada trade (~1ms)     ║
║  [FIX 2] Odds UP+DOWN em paralelo (threads simultâneas)  ║
║  [FIX 3] Ask price cacheado → sem REST na execução       ║
║  [FIX 4] Main loop: 20ms backup na zona crítica          ║
╠═══════════════════════════════════════════════════════════╣
║  Binance WS:  {ws:<26}         ║
║  Aposta:      ${self.cfg.base_bet:.0f} - ${self.cfg.max_bet:.0f} USDC                      ║
║  Stop loss:   ${self.cfg.max_daily_loss:.0f}/dia                          ║
║  Modo:        {mode:<24}         ║
╚═══════════════════════════════════════════════════════════╝""")

    def run(self):
        self._banner()
        self.binance.start()
        self.odds_cache.start()

        log.info("⏳ Aguardando dados Binance...")
        for _ in range(20):
            if self.buf.latest_price() > 0:
                break
            p = self.binance.get_price_rest()
            if p > 0:
                self.buf.add(Trade(time.time(), p, 0.001, True))
            time.sleep(1)

        if self.buf.latest_price() <= 0:
            log.error("❌ Sem dados de preço. Verifique conexão.")
            return

        log.info(f"✅ BTC: ${self.buf.latest_price():,.2f}")
        log.info("🚀 Bot v3.1 rodando!")

        try:
            while self._running:
                try:
                    self._run_cycle()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    log.error(f"Erro no ciclo: {e}", exc_info=True)

                # ── [FIX 4] Sleep adaptivo mínimo ──
                tr = 9999
                if self.current_market:
                    tr = self._time_remaining(self.current_market)

                if tr <= self.cfg.reversal_start:
                    # Zona crítica: main loop como backup do event-driven
                    # 20ms = taxa máxima sem travar a CPU
                    time.sleep(0.020)
                elif tr <= 30:
                    time.sleep(0.30)   # 300ms perto da zona
                elif tr <= 90:
                    time.sleep(1.0)
                else:
                    time.sleep(2.0)

        except KeyboardInterrupt:
            log.info("\n🛑 Bot parado")
        finally:
            self.binance.stop()
            self.odds_cache.stop()
            self._print_summary()

    def _print_summary(self):
        total = self.risk.total_wins + self.risk.total_losses
        wr = self.risk.total_wins / total * 100 if total > 0 else 0
        mode = "LIVE 💰" if not self.cfg.dry_run else "DRY RUN 🏃"
        print(f"""
╔══════════════════════════════════════════════╗
║  📊  Resumo Final  [{mode}]
╠══════════════════════════════════════════════╣
║  Apostas: {total:<6}  │  WR: {wr:<5.1f}%              ║
║  Wins:    {self.risk.total_wins:<6}  │  Losses: {self.risk.total_losses:<6}        ║
║  PnL:     ${self.risk.daily_pnl:<+10.2f}                    ║
╚══════════════════════════════════════════════╝""")
        self.learner.print_report()   # 🧠 Relatório do aprendizado


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    PolyBot().run()
