"""
Module: strategy_ai
Description: Simplified, highly liquid trend-following evaluation matrix.
             Loosened conditional constraints enable active paper-trading deployment
             while generating consistent optimization data for the ML engine.
"""

import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("AgentLogger")

class SniperStrategyAI:
    def __init__(self, min_score_threshold: float = 70.0):
        # Dropped from 100 to 70 to allow high-probability partial matches to execute
        self.min_score_threshold = min_score_threshold

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Vectorized Exponential Moving Average initialization function."""
        if len(prices) < period:
            return [prices[-1]] * len(prices) if prices else [0.0] * period
        emas = [0.0] * len(prices)
        emas[period - 1] = sum(prices[:period]) / period
        k = 2 / (period + 1)
        for i in range(period, len(prices)):
            emas[i] = (prices[i] * k) + (emas[i - 1] * (1 - k))
        return emas

    def _calculate_rsi(self, prices: List[float], period: int = 14) -> List[float]:
        """Calculates default 14-period Relative Strength Index over pricing arrays."""
        if len(prices) < period + 1:
            return [50.0] * len(prices)
        rsi = [50.0] * len(prices)
        gains = []
        losses = []
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
            
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = 100.0 - (100.0 / (1.0 + rs))
            
        for i in range(period + 1, len(prices)):
            gain = max(prices[i] - prices[i - 1], 0)
            loss = max(prices[i - 1] - prices[i], 0)
            avg_gain = ((avg_gain * (period - 1)) + gain) / period
            avg_loss = ((avg_loss * (period - 1)) + loss) / period
            if avg_loss == 0:
                rsi[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi[i] = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    def _calculate_macd(self, prices: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """Calculates standard MACD (12, 26, 9) signal lines and histograms."""
        ema12 = self._calculate_ema(prices, 12)
        ema26 = self._calculate_ema(prices, 26)
        macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
        signal_line = self._calculate_ema(macd_line, 9)
        histogram = [m - s for m, s in zip(macd_line, signal_line)]
        return macd_line, signal_line, histogram

    def generate_sniper_signal(
        self, 
        candles_1d: List[List[float]], 
        candles_1h: List[List[float]], 
        candles_15m: List[List[float]],
        adaptive_weights: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Evaluates multi-timeframe trends using flexible parameters 
        optimized for high-frequency algorithmic learning models.
        """
        result = {
            "action": "IGNORE",
            "trend_direction": "RANGING",
            "rsi_value": 50.0,
            "adx_value": 0.0,
            "raw_score": 0.0,
            "ai_score": 0.0,
            "entry_price": 0.0,
            "sl_price": 0.0,
            "tp_price": 0.0
        }

        # Safe length structural sanity checks
        if len(candles_1d) < 50 or len(candles_1h) < 30 or len(candles_15m) < 15:
            return result

        # Pull closed array segments targeting active tracking index [-1]
        closes_1d = [c[4] for c in candles_1d]
        closes_1h = [c[4] for c in candles_1h]
        closes_15m = [c[4] for c in candles_15m]

        # STEP 1: Macro Trend Filter (1D Frame - Direct Price vs EMA Cross)
        ema50_1d = self._calculate_ema(closes_1d, 50)[-1]
        current_price = closes_15m[-1]

        if current_price >= ema50_1d:
            trend = "LONG"
        else:
            trend = "SHORT"

        result["trend_direction"] = trend
        score = 0.0

        # STEP 2: Loosened Structural Pullback Check (1H Frame)
        rsi_1h = self._calculate_rsi(closes_1h, 14)[-1]
        result["rsi_value"] = rsi_1h
        
        # Widened parameters: Gives the asset breathing room to score points
        if trend == "LONG":
            if rsi_1h <= 55.0:  # Loosened floor from 45 to 55 to catch minor dips
                score += 40.0
        elif trend == "SHORT":
            if rsi_1h >= 45.0:  # Loosened ceiling from 55 to 45
                score += 40.0

        # STEP 3: Momentum Trigger Vector (15M Frame)
        _, _, macd_hist_15m = self._calculate_macd(closes_15m)
        current_hist = macd_hist_15m[-1]
        prev_hist = macd_hist_15m[-2]

        if trend == "LONG":
            # Condition A: Active Bullish crossover or general upward acceleration
            if current_hist > prev_hist:
                score += 40.0
            # Condition B: Core Volume amplification confirmation
            if candles_15m[-1][5] > candles_15m[-2][5]:
                score += 20.0
                
        elif trend == "SHORT":
            # Condition A: Active Bearish crossover or downward decay momentum
            if current_hist < prev_hist:
                score += 40.0
            # Condition B: Core Volume amplification confirmation
            if candles_15m[-1][5] > candles_15m[-2][5]:
                score += 20.0

        result["raw_score"] = score
        
        # Apply ML database adjustment modifiers dynamically
        multiplier = adaptive_weights.get("volatility_buffer", 1.0)
        ai_score = score * multiplier
        result["ai_score"] = ai_score

        # Verification gate: if final score passes target, trigger virtual execution
        if ai_score >= self.min_score_threshold:
            result["action"] = "EXECUTE"
            result["entry_price"] = current_price
            
            # 5:1 Risk Reward Ratio Target Matrices
            if trend == "LONG":
                result["sl_price"] = current_price * 0.95
                result["tp_price"] = current_price * 1.25
            else:
                result["sl_price"] = current_price * 1.05
                result["tp_price"] = current_price * 0.75

        return result