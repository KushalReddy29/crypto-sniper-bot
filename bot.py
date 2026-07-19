"""
Module: bot
Description: Finalized production entry point for the AI Crypto Sniper Agent.
             Orchestrates parallel matrix scanning, automated SL/TP isolation,
             async Telegram alerts, and an embedded HTTP listener for cloud port-binding.
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
    """Tiny dummy HTTP handler to satisfy Render's port check rules."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy", "agent": "active"}')

    def log_message(self, format, *args):
        return  # Suppress internal web logging to keep console clean

def run_dummy_web_server():
    """Binds to the port provided by Render to prevent deployment timeout crashes."""
    # Render passes down the designated port via environment variable automatically
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), RenderHealthCheckServer)
    logger.info(f"[PORT BINDING] Health check web server listening on port {port}...")
    server.serve_forever()

# --- TELEGRAM DISPATCH ALERTS ---
async def dispatch_telegram_alert(symbol: str, score: float, entry: float, sl: float, tp: float, mode: str):
    """Dispatches real-time setup alerts to the designated Telegram channel via non-blocking async POST requests."""
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    
    if not token or not chat_id or token == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("[TELEGRAM] Notification skipped. Credentials missing from environment variables.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    message_text = (
        f"🎯 **🚨 SNIPER SIGNAL ISOLATED** 🚨\n\n"
        f"▪️ **Asset:** #{symbol.replace('/', '_')}\n"
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
                    logger.error(f"[TELEGRAM] Delivery API rejection ({response.status}): {res_text}")
    except Exception as e:
        logger.error(f"[TELEGRAM] Failed to send socket notification packet: {e}")

# --- CORE SCANNING EXECUTION CORE ---
async def process_single_symbol(symbol: str, exchange: CoinDCXExchangeEngine, strategy: SniperStrategyAI, db: QuantumDatabaseManager, mode: str):
    """Worker task to fetch data, score metrics, calculate SL/TP brackets, and record setups concurrently."""
    try:
        timeframe = "1m"
        
        # 1. Fetch clean candles via the patched non-blocking exchange layer
        candles = await exchange.fetch_clean_ohlcv(symbol, timeframe, limit=100)
        if not candles:
            return  
            
        # 2. Run strategy evaluation logic
        score = await strategy.analyze_asset(symbol, candles) 
        
        # 3. Handle setup isolation if threshold conditions are reached
        if score >= strategy.min_score_threshold:
            current_close = float(candles[-1][4])
            
            # Formulate structural risk brackets (5% SL / 25% TP for Long configurations)
            stop_loss = current_close * 0.95
            take_profit = current_close * 1.25
            
            logger.info(
                f"🏆 Champion Setup Isolated: {symbol}\n"
                f"   | Score: {score}% | Entry: {current_close} | SL: {stop_loss:.4f} | TP: {take_profit:.4f}"
            )
            
            # Record the execution profile inside your SQLite tracking layers
            await db.record_position_entry(symbol, score)
            
            # Fire the async Telegram alert to the chat group in the background
            asyncio.create_task(dispatch_telegram_alert(symbol, score, current_close, stop_loss, take_profit, mode))
            
    except Exception as e:
        pass

async def run_scanning_loop(db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, dynamic_watchlist: List[str], mode: str):
    """Optimized parallel processing engine scanning targets simultaneously using async tasks."""
    logger.info(f"[SCANNER] Launching batch-managed parallel sweep across {len(dynamic_watchlist)} contracts...")
    
    sem = asyncio.Semaphore(20) 
    
    async def safe_worker(symbol: str):
        async with sem:
            await process_single_symbol(symbol, exchange, strategy, db, mode)
            
    tasks = [safe_worker(symbol) for symbol in dynamic_watchlist]
    await asyncio.gather(*tasks)

async def main():
    """Main lifecycle manager stabilizing the operational pipeline."""
    logger.info("[ENGINE HEARTBEAT] Booting up crypto trading engine...")
    
    # Fire up the dummy port listener in a background thread so Render stops timing out
    web_thread = threading.Thread(target=run_dummy_web_server, daemon=True)
    web_thread.start()
    
    db = QuantumDatabaseManager()
    await db.initialize_tables()
    logger.info("[DATABASE] CoinDCX tracking registers ready.")
    
    strategy = SniperStrategyAI(min_score_threshold=settings.MIN_SCORE_THRESHOLD)
    logger.info("[DATABASE] Machine learning strategy weights table seeded successfully.")
    
    exchange = CoinDCXExchangeEngine(
        api_key=settings.COINDCX_API_KEY,
        api_secret=settings.COINDCX_API_SECRET,
        use_testnet=settings.USE_TESTNET
    )
    
    mode = "PAPER" if settings.USE_TESTNET else "LIVE"
    logger.info(f"[ENGINE HEARTBEAT] System operating mode armed: {mode}")
    
    while True:
        try:
            # Set the explicit watch matrix to verify scan performance and precision
            dynamic_watchlist = ["HUMA/USDT", "BANK/USDT", "ZEC/USDT"]
            
            await run_scanning_loop(db, strategy, exchange, dynamic_watchlist, mode)
            
            logger.info(f"[ENGINE HEARTBEAT] Sweep finished. Cooling down for 60 seconds...")
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("[ENGINE HEARTBEAT] Shutting down agent tracking pipelines...")
            break
        except Exception as e:
            logger.error(f"[CRITICAL APPLICATION FAULT] Core daemon loop crashed: {e}")
            await asyncio.sleep(10)  

if __name__ == "__main__":
    asyncio.run(main())