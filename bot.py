"""
Module: bot
Description: Finalized production execution entry point for the AI Crypto Sniper Agent.
             Orchestrates parallel matrix scanning across hardcoded high-liquidity 
             CoinDCX futures tokens, executing 5%/25% risk bracket logic on a 0-threshold layout.
"""

import asyncio
import logging
import time
import sys
import os
import aiohttp
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from typing import List

from config_engine import settings
from database_layer import QuantumDatabaseManager
from exchange_layer import CoinDCXExchangeEngine
from strategy_ai import SniperStrategyAI

# Configure structured system logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AgentLogger")

# --- RENDER PORT BINDING COMPLIANCE ENGINE ---
class RenderHealthCheckServer(BaseHTTPRequestHandler):
    """Tiny background HTTP handler to satisfy Render's web service routing checks."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy", "agent": "active"}')

    def log_message(self, format, *args):
        return  # Suppress background traffic logs to keep terminal readable

def run_dummy_web_server():
    """Binds to the environment-assigned port to prevent web deployment timeout failures."""
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), RenderHealthCheckServer)
    logger.info(f"[PORT BINDING] Health check web server listening on port {port}...")
    server.serve_forever()

# --- TELEGRAM DISPATCH ALERTS ---
async def dispatch_telegram_alert(symbol: str, score: float, entry: float, sl: float, tp: float, mode: str):
    """Sends immediate Markdown strategy alert payloads to the Telegram API channel."""
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    
    if not token or not chat_id or token == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("[TELEGRAM] Notification skipped. Credentials missing in config profile.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    # Format symbol cleanly for telegram tags (e.g., B-BTC_USDT -> BTC_USDT)
    clean_tag = symbol.replace("B-", "")
    
    message_text = (
        f"🎯 **🚨 SNIPER SIGNAL ISOLATED** 🚨\n\n"
        f"▪️ **Asset:** #{clean_tag}\n"
        f"▪️ **Mode:** `{mode}`\n"
        f"▪️ **Match Probability:** `{score}%`\n\n"
        f"📈 **Execution Matrix:**\n"
        f"🔹 **Entry Price:** `{entry:.4f}`\n"
        f"🛑 **Stop Loss (SL):** `{sl:.4f}`\n"
        f"🎯 **Take Profit (TP):** `{tp:.4f}`"
    )

    payload = {
        "chat_id": chat_id,
        "text": message_text,
        "parse_mode": "Markdown"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    logger.info(f"[TELEGRAM] Alert broadcast successfully for {symbol}.")
                else:
                    res_text = await response.text()
                    logger.error(f"[TELEGRAM] Delivery rejection ({response.status}): {res_text}")
    except Exception as e:
        logger.error(f"[TELEGRAM] Failed to send socket notification packet: {e}")

# --- CORE SCANNING EXECUTION CORE ---
async def process_single_symbol(symbol: str, exchange: CoinDCXExchangeEngine, strategy: SniperStrategyAI, db: QuantumDatabaseManager, mode: str):
    """Task worker to execute concurrent candlestick queries and process risk parameters."""
    try:
        timeframe = "1m"
        
        # 1. Pull down transaction candles via async thread pooling
        candles = await exchange.fetch_clean_ohlcv(symbol, timeframe, limit=100)
        if not candles:
            return  
            
        # 2. Extract technical alignment scores
        score = await strategy.analyze_asset(symbol, candles) 
        
        # 3. Process matches against forced 0 threshold layout
        if score >= strategy.min_score_threshold:
            current_close = float(candles[-1][4])
            
            # Mathematical evaluation of risk frameworks (5% SL / 25% TP)
            stop_loss = current_close * 0.95
            take_profit = current_close * 1.25
            
            logger.info(
                f"🏆 Champion Setup Isolated: {symbol}\n"
                f"   | Score: {score}% | Entry: {current_close} | SL: {stop_loss:.4f} | TP: {take_profit:.4f}"
            )
            
            # Record tracking variables in local databases (Simulation/Paper Trading)
            await db.record_position_entry(symbol, score)
            
            # Trigger independent async alert task for Telegram
            asyncio.create_task(dispatch_telegram_alert(symbol, score, current_close, stop_loss, take_profit, mode))
            
            # LIVE ORDERS: Submits market placement directly through API credentials
            position_size = 1.0  
            await exchange.execute_sniper_trade(symbol, "LONG", position_size, current_close, stop_loss, take_profit)
            
    except Exception as e:
        logger.error(f"[SCANNER WORKER ERROR] Failed executing analysis for {symbol}: {e}")

async def run_scanning_loop(db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, watchlist: List[str], mode: str):
    """Orchestrates high-concurrency loops across asset array using throttling semaphores."""
    logger.info(f"[SCANNER] Launching parallel sweep across {len(watchlist)} contracts...")
    
    sem = asyncio.Semaphore(20) 
    
    async def safe_worker(symbol: str):
        async with sem:
            await process_single_symbol(symbol, exchange, strategy, db, mode)
            
    tasks = [safe_worker(symbol) for symbol in watchlist]
    await asyncio.gather(*tasks)

async def main():
    """Bootstraps background compliance services, database rules, and trading loops."""
    logger.info("[ENGINE HEARTBEAT] Booting up crypto trading engine...")
    
    # Launch network port-binding HTTP service in a background worker thread
    web_thread = threading.Thread(target=run_dummy_web_server, daemon=True)
    web_thread.start()
    
    db = QuantumDatabaseManager()
    await db.initialize_tables()
    logger.info("[DATABASE] CoinDCX tracking registers ready.")
    
    # Instantiate strategy configured with forced zero evaluation parameters
    strategy = SniperStrategyAI(min_score_threshold=0)
    logger.info("[STRATEGY] AI engine armed with forced 0 threshold matrix rules.")
    
    exchange = CoinDCXExchangeEngine(
        api_key=settings.COINDCX_API_KEY,
        api_secret=settings.COINDCX_API_SECRET,
        use_testnet=settings.USE_TESTNET
    )
    
    mode = "PAPER" if settings.USE_TESTNET else "LIVE"
    logger.info(f"[ENGINE HEARTBEAT] System operating mode armed: {mode}")
    
    # Target tokens formatted to explicitly map to CoinDCX endpoint targets
    verified_watchlist = ["B-BTC_USDT", "B-ETH_USDT", "B-ZEC_USDT"]
    
    while True:
        try:
            await run_scanning_loop(db, strategy, exchange, verified_watchlist, mode)
            
            logger.info(f"[ENGINE HEARTBEAT] Sweep finished. Cooling down for 60 seconds...")
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("[ENGINE HEARTBEAT] Shutting down agent tracking pipelines...")
            break
        except Exception as e:
            logger.error(f"[CRITICAL APPLICATION FAULT] Core daemon loop encountered error: {e}")
            await asyncio.sleep(10)  

if __name__ == "__main__":
    asyncio.run(main())