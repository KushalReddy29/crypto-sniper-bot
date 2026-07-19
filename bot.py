"""
Module: bot
Description: Production entry point and main orchestration loop for the AI Crypto Trading Agent.
             Implements high-frequency concurrent scanning across institutional watcher matrices.
"""

import asyncio
import logging
import time
import sys
from typing import List

# Import your configuration, database, exchange, and strategy layers
# Make sure these module names match your project files exactly
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

async def process_single_symbol(symbol: str, exchange: CoinDCXExchangeEngine, strategy: SniperStrategyAI, db: QuantumDatabaseManager, mode: str):
    """Worker task to fetch data, score metrics, and record setups for an individual token."""
    try:
        timeframe = "1m"  # Standard high-frequency window
        
        # 1. Fetch clean candles via the patched non-blocking exchange layer
        candles = await exchange.fetch_clean_ohlcv(symbol, timeframe, limit=100)
        if not candles:
            return  # Gracefully skip if invalid/422/empty
            
        # 2. Run your strategy evaluation logic
        # Note: If your strategy method has a slightly different name, match it here
        score = await strategy.analyze_asset(symbol, candles) 
        
        # 3. Handle setup isolation if threshold conditions are reached
        if score >= strategy.min_score_threshold:
            logger.info(f"🏆 Champion Setup Isolated: {symbol} with probability metric: {score}")
            
            # Record the position cleanly in the local database registry
            await db.record_position_entry(symbol, score)
            
            # --- TELEGRAM DISPATCH TRIGGER ---
            # If your telegram alert system is active, call it here:
            # await telegram.send_signal(symbol, score, mode)
            
    except Exception as e:
        # Suppress individual token failures to protect the rest of the concurrent batch execution
        pass

async def run_scanning_loop(db: QuantumDatabaseManager, strategy: SniperStrategyAI, exchange: CoinDCXExchangeEngine, dynamic_watchlist: List[str], mode: str):
    """Optimized parallel processing engine scanning hundreds of pairs simultaneously using async tasks."""
    logger.info(f"[SCANNER] Launching batch-managed parallel sweep across {len(dynamic_watchlist)} contracts...")
    
    # Caps concurrent network requests to prevent CoinDCX from blocking your cloud IP
    sem = asyncio.Semaphore(20) 
    
    async def safe_worker(symbol: str):
        async with sem:
            await process_single_symbol(symbol, exchange, strategy, db, mode)
            
    # Spawn background concurrent processing tasks for every asset in the matrix
    tasks = [safe_worker(symbol) for symbol in dynamic_watchlist]
    
    # Fire off all 482+ requests in parallel execution matrix
    await asyncio.gather(*tasks)

async def main():
    """Main lifecycle manager stabilizing the operational pipeline."""
    logger.info("[ENGINE HEARTBEAT] Booting up crypto trading engine...")
    
    # 1. Instantiate the database manager and enforce data schemas
    db = QuantumDatabaseManager()
    await db.initialize_tables()
    logger.info("[DATABASE] CoinDCX tracking registers ready.")
    
    # 2. Instantiate the institutional core strategy matrix 
    strategy = SniperStrategyAI(min_score_threshold=settings.MIN_SCORE_THRESHOLD)
    logger.info("[DATABASE] Machine learning strategy weights table seeded successfully.")
    
    # 3. Mount secure credentials onto the exchange execution framework
    exchange = CoinDCXExchangeEngine(
        api_key=settings.COINDCX_API_KEY,
        api_secret=settings.COINDCX_API_SECRET,
        use_testnet=settings.USE_TESTNET
    )
    
    mode = "PAPER" if settings.USE_TESTNET else "LIVE"
    logger.info(f"[ENGINE HEARTBEAT] System operating mode armed: {mode}")
    
    # 4. Enter the infinite high-frequency orchestration execution loop
    while True:
        try:
            # Query the master registry for active exchange symbols
            # Note: Ensure this function maps directly to your watchlist query structure
            dynamic_watchlist = await exchange.get_active_futures_watchlist()
            
            # Trigger the rapid parallelized market scan matrix
            await run_scanning_loop(db, strategy, exchange, dynamic_watchlist, mode)
            
            # Cool down for 60 seconds before initiating the next matrix sweep
            logger.info(f"[ENGINE HEARTBEAT] Sweep finished. Cooling down for 60 seconds...")
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("[ENGINE HEARTBEAT] Shutting down agent tracking pipelines...")
            break
        except Exception as e:
            logger.error(f"[CRITICAL APPLICATION FAULT] Core daemon loop crashed: {e}")
            await asyncio.sleep(10)  # Safe delay before self-healing and restart

if __name__ == "__main__":
    asyncio.run(main())