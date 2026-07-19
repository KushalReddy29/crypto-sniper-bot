"""
Module: database_layer
Description: Asynchronous SQLite database management layer for the sniper agent.
             Handles signal recording, trade tracking, and machine learning parameter feedback loops.
"""

import os
import json
import logging
import aiosqlite
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger("AgentLogger")

class QuantumDatabaseManager:
    def __init__(self, db_path: str = "coindcx_agent_registry.db"):
        self.db_path = db_path

    async def initialize_tables(self):
        """Initializes tables and seeds default baseline machine learning weights."""
        async with aiosqlite.connect(self.db_path) as db:
            # 1. Signals Logging Table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    action TEXT,
                    trend_direction TEXT,
                    rsi_value REAL,
                    ai_score REAL,
                    raw_data TEXT
                )
            """)

            # 2. Trade Ledger Tracking Table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trade_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER,
                    ticket_id TEXT UNIQUE,
                    symbol TEXT,
                    exe_type TEXT,
                    direction TEXT,
                    status TEXT,
                    entry_price REAL,
                    sl_price REAL,
                    tp_price REAL,
                    position_size REAL,
                    exit_price REAL,
                    final_pnl REAL,
                    opened_at TEXT,
                    closed_at TEXT,
                    FOREIGN KEY(signal_id) REFERENCES market_signals(id)
                )
            """)

            # 3. Dynamic Strategy Weights Table (The Machine Learning Store)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS strategy_weights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extreme_rsi_multiplier REAL,
                    volatility_buffer REAL,
                    learning_rate REAL
                )
            """)
            await db.commit()

            # Seed default weights if the table is freshly initialized
            async with db.execute("SELECT COUNT(*) FROM strategy_weights") as cursor:
                row = await cursor.fetchone()
                if row and row[0] == 0:
                    await db.execute("""
                        INSERT INTO strategy_weights (id, extreme_rsi_multiplier, volatility_buffer, learning_rate)
                        VALUES (1, 0.95, 1.0, 0.05)
                    """)
                    await db.commit()
                    logger.info("[DATABASE] Machine learning strategy weights table seeded successfully.")
        
        logger.info("[DATABASE] CoinDCX tracking registers ready.")

    async def get_adaptive_weights(self) -> Dict[str, float]:
        """Retrieves the current dynamic strategic parameters for indicator scaling."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT extreme_rsi_multiplier, volatility_buffer, learning_rate FROM strategy_weights WHERE id = 1") as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
        # Resilient fallback defaults
        return {"extreme_rsi_multiplier": 0.95, "volatility_buffer": 1.0, "learning_rate": 0.05}

    async def record_market_signal(self, analysis: Dict[str, Any]) -> int:
        """Logs processed mathematical matrix tracking configurations for audited performance tracking."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO market_signals (timestamp, symbol, action, trend_direction, rsi_value, ai_score, raw_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().isoformat(),
                    analysis["symbol"],
                    analysis["action"],
                    analysis["trend_direction"],
                    analysis["rsi_value"],
                    analysis["ai_score"],
                    json.dumps(analysis)
                )
            )
            await db.commit()
            return cursor.lastrowid

    async def record_trade_open(self, signal_id: int, symbol: str, exe_type: str, direction: str, rules: Dict[str, Any], ticket_id: str):
        """Commits newly filled live tracking positions directly into the structural trade ledger registries."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO trade_ledger (
                    signal_id, ticket_id, symbol, exe_type, direction, status, 
                    entry_price, sl_price, tp_price, position_size, opened_at
                   ) VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?)""",
                (
                    signal_id,
                    str(ticket_id),
                    symbol,
                    exe_type,
                    direction,
                    float(rules["entry"]),
                    float(rules["sl"]),
                    float(rules["tp"]),
                    float(rules["size"]),
                    datetime.now().isoformat()
                )
            )
            await db.commit()
            logger.info(f"[DATABASE] Position entry recorded cleanly in ledger for {symbol}. ID: {ticket_id}")

    async def fetch_all_active_trades(self) -> List[Dict[str, Any]]:
        """Retrieves all active open trades currently monitored in the system."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trade_ledger WHERE status = 'ACTIVE'") as cursor:
                rows = await cursor.fetchall()
                trades = []
                for row in rows:
                    t_dict = dict(row)
                    # Unpack nested execution metrics structure for the bracket validation scanner
                    t_dict["rules"] = {
                        "entry": t_dict["entry_price"],
                        "sl": t_dict["sl_price"],
                        "tp": t_dict["tp_price"],
                        "size": t_dict["position_size"]
                    }
                    trades.append(t_dict)
                return trades

    async def record_trade_close(self, trade_id: int, exit_price: float, final_pnl: float):
        """Marks a monitored tracking matrix trade as closed once an endpoint boundary is breached."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE trade_ledger 
                   SET status = 'CLOSED', exit_price = ?, final_pnl = ?, closed_at = ?
                   WHERE id = ?""",
                (float(exit_price), float(final_pnl), datetime.now().isoformat(), trade_id)
            )
            await db.commit()
            logger.info(f"[DATABASE] Closed trade ID {trade_id} marked resolved at exit price {exit_price}.")

    async def apply_machine_learning_feedback(self, trade_id: int, outcome: str, pnl_percentage: float):
        """
        Self-Correction Matrix: Adjusts trading parameters dynamically based on outcomes.
        If a trade loses, it automatically tightens constraints; if it wins, it expands parameters safely.
        """
        weights = await self.get_adaptive_weights()
        lr = weights.get("learning_rate", 0.05)
        
        if outcome == "LOSS":
            # Mistake identified: Reduce the multiplier to force stricter RSI entry requirements next time
            new_rsi_mult = max(0.70, weights["extreme_rsi_multiplier"] - lr)
            # Expand volatility safety buffer padding requirements
            new_vol_buf = min(1.40, weights["volatility_buffer"] + (lr * 1.5))
            logger.info(f"[ML ENGINE] Adjusting filters strictly. RSI Mult -> {new_rsi_mult:.3f}, Vol Buffer -> {new_vol_buf:.3f}")
        else:
            # Success confirmed: Reward structural parameters by gently optimizing threshold metrics
            new_rsi_mult = min(1.00, weights["extreme_rsi_multiplier"] + (lr * 0.5))
            new_vol_buf = max(0.80, weights["volatility_buffer"] - lr)
            logger.info(f"[ML ENGINE] Rewarding parameters. RSI Mult -> {new_rsi_mult:.3f}, Vol Buffer -> {new_vol_buf:.3f}")

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE strategy_weights 
                   SET extreme_rsi_multiplier = ?, volatility_buffer = ? 
                   WHERE id = 1""",
                (new_rsi_mult, new_vol_buf)
            )
            await db.commit()
            logger.info("[DATABASE] Strategy adaptive weight optimization committed to disk.")