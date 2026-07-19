"""
Module: bot
Description: Ultra-high-velocity parallel execution engine with integrated
             semaphore safeguards to prevent Cloudflare 429 / 1015 rate-limiting.
"""

import asyncio
import logging
import aiohttp
import requests
from config_engine import settings
from database_layer import QuantumDatabaseManager
from strategy_ai import SniperStrategyAI
from exchange_layer import CoinDCXExchangeEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("AgentLogger")

RUN_TIME_BLACKLIST = set()

# Crucial Guardrail: Limits maximum simultaneous connections to prevent bans
API_SEMAPHORE = asyncio.Semaphore(20) 

async def send_telegram_alert(message: str):
    if not settings.TELEGRAM_ENABLED or not settings.TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": settings.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=5) as resp:
                if resp.status != 200:
                    logger.debug(f"[TELEGRAM] Alert failed status: {resp.status}")
    except Exception as e:
        logger.debug(f"[TELEGRAM] Network error routing push notification: {e}")

def fetch_all_active_futures_pairs() -> list:
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            instruments = resp.json()
            pairs = []
            target_list = instruments if isinstance(instruments, list) else instruments.get("data", [])
            for inst in target_list:
                if isinstance(inst, dict):
                    raw_pair = inst.get("pair") or inst.get("symbol") or ""
                elif isinstance(inst, str):
                    raw_pair = inst
                else:
                    continue
                if raw_pair and raw_pair.startswith("B-"):
                    clean = raw_pair.replace("B-", "").replace("_", "/")
                    if "USDT" in clean and clean not in pairs:
                        pairs.append(clean)
            return pairs
    except Exception as e:
        logger.error(f"[MARKET SYNC] Failed tracking global instruments mapping: {e}")
    return ["BTC/USDT", "ETH/USDT", "SOL/USDT", "LINK/USDT"]

async def process_single_coin(symbol: str, db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, adaptive_weights: dict) -> dict:
    if symbol in RUN_TIME_BLACKLIST:
        return None

    # The traffic cop controls entry here
    async with API_SEMAPHORE:
        try:
            # Minor micro-pacing offset inside the batch to soften traffic spikes
            await asyncio.sleep(0.05)
            
            candles_1d = await exchange.fetch_clean_ohlcv(symbol, "1d", limit=100)
            if not candles_1d:
                RUN_TIME_BLACKLIST.add(symbol)
                return None
                
            candles_1h = await exchange.fetch_clean_ohlcv(symbol, "1h", limit=60)
            candles_15m = await exchange.fetch_clean_ohlcv(symbol, "15m", limit=30)
            
            if not candles_1h or not candles_15m:
                return None

            analysis = strategy.generate_sniper_signal(candles_1d, candles_1h, candles_15m, adaptive_weights)
            analysis["symbol"] = symbol
            
            signal_id = await db.record_market_signal(analysis)
            analysis["signal_id"] = signal_id
            
            if analysis["action"] == "EXECUTE":
                return analysis
                
        except Exception:
            pass
    return None

async def run_scanning_loop(db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, dynamic_watchlist: list, mode: str):
    active_trades = await db.fetch_all_active_trades()
    if len(active_trades) >= settings.MAX_OPEN_POSITIONS:
        return

    adaptive_weights = await db.get_adaptive_weights()
    wallet_balance = 10000.0 if mode == "PAPER" else await exchange.get_free_balance("USDT")
    
    if wallet_balance <= 0:
        return

    tasks = [
        process_single_coin(symbol, db, strategy, exchange, adaptive_weights)
        for symbol in dynamic_watchlist
    ]
    
    logger.info(f"[SCANNER] Launching batch-managed parallel sweep across {len(tasks)} contracts...")
    results = await asyncio.gather(*tasks)
    
    valid_candidates = [res for res in results if res is not None]

    if not valid_candidates:
        logger.info("[SCANNER] Safe sweep complete. No coins met entry thresholds.")
        return

    valid_candidates.sort(key=lambda x: x["ai_score"], reverse=True)
    champion = valid_candidates[0]
    symbol = champion["symbol"]
    
    logger.info(f"🏆 Champion Setup Isolated: {symbol} with probability metric: {champion['ai_score']}")
    
    try:
        target_entry = champion["entry_price"]
        risk_usd = wallet_balance * settings.RISK_PER_TRADE_PCT
        raw_size = risk_usd / (target_entry * 0.05)
        
        precision = await exchange.get_market_precision_rules(symbol)
        precise_size = round(raw_size, precision["amount_precision"])
        
        trade_rules = {
            "entry": target_entry, "sl": champion["sl_price"], "tp": champion["tp_price"], "size": precise_size
        }
        
        if mode == "PAPER":
            ticket_id = f"PAPER_{int(asyncio.get_event_loop().time())}"
            await db.record_trade_open(
                signal_id=champion["signal_id"], symbol=symbol, exe_type="PAPER",
                direction=champion["trend_direction"], rules=trade_rules, ticket_id=ticket_id
            )
            await send_telegram_alert(f"📝 *Virtual Paper Trade Filled:* `{symbol}` ({champion['trend_direction']}) | Score: `{champion['ai_score']}`")
        else:
            receipt = await exchange.execute_sniper_trade(
                symbol=symbol, direction=champion["trend_direction"], position_size=precise_size,
                entry_price=target_entry, stop_loss_price=champion["sl_price"], take_profit_price=champion["tp_price"]
            )
            if receipt:
                await db.record_trade_open(
                    signal_id=champion["signal_id"], symbol=symbol, exe_type="LIVE",
                    direction=champion["trend_direction"], rules=trade_rules, ticket_id=receipt["entry_ticket"]
                )
                await send_telegram_alert(f"🚀 *LIVE Real Money Order Executed:* `{symbol}` | Size: `{precise_size}`")
    except Exception as e:
        logger.error(f"Execution handling failure on asset {symbol}: {e}")

async def main():
    mode = getattr(settings, "EXECUTION_MODE", "PAPER").upper()
    logger.info(f"Initializing Guarded Parallel Engine Matrix Framework in [{mode}] Mode...")
    
    db = QuantumDatabaseManager()
    await db.initialize_tables()
    
    strategy = SniperStrategyAI(min_score_threshold=settings.MIN_SCORE_THRESHOLD)
    exchange = CoinDCXExchangeEngine(
        api_key=settings.COINDCX_API_KEY, api_secret=settings.COINDCX_API_SECRET, use_testnet=settings.USE_TESTNET
    )
    
    await send_telegram_alert(f"🤖 *System Bootstrap Confirmed:* Safe Parallel Agent Online in [{mode}] Mode.")

    try:
        while True:
            dynamic_watchlist = fetch_all_active_futures_pairs()
            logger.info(f"[ENGINE HEARTBEAT] Initiating rate-safe network sweep over {len(dynamic_watchlist)} targets...")
            await run_scanning_loop(db, strategy, exchange, dynamic_watchlist, mode)
            
            logger.info("[ENGINE HEARTBEAT] Sweep finished. Cooling down for 60 seconds...")
            await asyncio.sleep(60)
    finally:
        logger.info("Agent offline loop terminated clean.")

if __name__ == "__main__":
    asyncio.run(main())