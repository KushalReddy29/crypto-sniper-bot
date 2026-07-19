"""
Module: bot
Description: Fully complete operational entry point for the AI Crypto Sniper Agent.
             Orchestrates dynamic asset discovery, parallel tracking matrices,
             active multi-bracket calculations, and live automated execution.
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
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy", "agent": "active"}')

    def log_message(self, format, *args):
        return

def run_dummy_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), RenderHealthCheckServer)
    logger.info(f"[PORT BINDING] Health check web server listening on port {port}...")
    server.serve_forever()

# --- TELEGRAM DISPATCH ALERTS ---
async def dispatch_telegram_alert(symbol: str, score: float, entry: float, sl: float, tp: float, mode: str):
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    
    if not token or not chat_id or token == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.warning("[TELEGRAM] Notification skipped. Credentials missing.")
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

    payload = {"chat_id": chat_id, "text": message_text, "parse_mode": "Markdown"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    logger.info(f"[TELEGRAM] Alert broadcast successfully for {symbol}.")
    except Exception as e:
        logger.error(f"[TELEGRAM] Failed to send socket notification packet: {e}")

# --- CORE SCANNING EXECUTION CORE ---
async def process_single_symbol(symbol: str, exchange: CoinDCXExchangeEngine, strategy: SniperStrategyAI, db: QuantumDatabaseManager, mode: str):
    try:
        timeframe = "1m"
        candles = await exchange.fetch_clean_ohlcv(symbol, timeframe, limit=100)
        if not candles:
            return  
            
        score = await strategy.analyze_asset(symbol, candles) 
        
        if score >= strategy.min_score_threshold:
            current_close = float(candles[-1][4])
            stop_loss = current_close * 0.95
            take_profit = current_close * 1.25
            
            logger.info(
                f"🏆 Champion Setup Isolated: {symbol} | Score: {score}% | Entry: {current_close} | SL: {stop_loss:.4f} | TP: {take_profit:.4f}"
            )
            
            # 1. Save setup to Paper Trading tracking ledger
            await db.record_position_entry(symbol, score)
            
            # 2. Fire the asynchronous Telegram mobile alert
            asyncio.create_task(dispatch_telegram_alert(symbol, score, current_close, stop_loss, take_profit, mode))
            
            # 3. WEAPONIZED: Execute live trade order on CoinDCX directly!
            position_size = 1.0  # Set size depending on allocation limits
            await exchange.execute_sniper_trade(symbol, "LONG", position_size, current_close, stop_loss, take_profit)
            
    except Exception as e:
        pass

async def run_scanning_loop(db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, dynamic_watchlist: List[str], mode: str):
    logger.info(f"[SCANNER] Launching parallel sweep across {len(dynamic_watchlist)} contracts...")
    sem = asyncio.Semaphore(20) 
    
    async def safe_worker(symbol: str):
        async with sem:
            await process_single_symbol(symbol, exchange, strategy, db, mode)
            
    tasks = [safe_worker(symbol) for symbol in dynamic_watchlist]
    await asyncio.gather(*tasks)

async def main():
    logger.info("[ENGINE HEARTBEAT] Booting up crypto trading engine...")
    
    web_thread = threading.Thread(target=run_dummy_web_server, daemon=True)
    web_thread.start()
    
    db = QuantumDatabaseManager()
    await db.initialize_tables()
    
    # Run with threshold = 0 to verify the entire pipeline with live instruments immediately
    strategy = SniperStrategyAI(min_score_threshold=0)
    
    exchange = CoinDCXExchangeEngine(
        api_key=settings.COINDCX_API_KEY,
        api_secret=settings.COINDCX_API_SECRET,
        use_testnet=settings.USE_TESTNET
    )
    
    mode = "PAPER" if settings.USE_TESTNET else "LIVE"
    logger.info(f"[ENGINE HEARTBEAT] System operating mode armed: {mode}")
    
    while True:
        try:
            # DYNAMIC DISCOVERY: Fetch every single real active futures contract running on CoinDCX!
            dynamic_watchlist = await exchange.fetch_active_futures_watchlist()
            
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