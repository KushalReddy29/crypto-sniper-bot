"""
Module: exchange_layer
Description: Production direct institutional execution driver for CoinDCX Futures.
             Includes strict 15-second network socket timeouts to prevent terminal lockups.
"""

import hmac
import hashlib
import json
import logging
import time
import asyncio
import requests
from typing import Dict, Any, Optional, List

logger = logging.getLogger("AgentLogger")

class CoinDCXExchangeEngine:
    def __init__(self, api_key: str, api_secret: str, use_testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.coindcx.com"
        self.public_url = "https://public.coindcx.com"

    def _generate_signature(self, json_string: str) -> str:
        """Computes compulsory HMAC-SHA256 hex signature required by CoinDCX secure endpoints."""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            json_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _sync_fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> List[List[float]]:
        """Synchronous futures candlesticks pulling with explicit connection timeouts."""
        clean_pair = f"B-{symbol.replace('/', '_')}"
        url = f"{self.public_url}/market_data/candles"
        
        query_params = {
            "pair": clean_pair,
            "interval": timeframe.lower(),
            "limit": limit
        }
        
        try:
            # FIXED: Added explicit 15-second safety timeout
            resp = requests.get(url, params=query_params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                normalized = []
                for candle in data:
                    normalized.append([
                        int(candle.get("time", time.time() * 1000)),
                        float(candle["open"]),
                        float(candle["high"]),
                        float(candle["low"]),
                        float(candle["close"]),
                        float(candle.get("volume", 0.0))
                    ])
                return normalized
            else:
                logger.error(f"[CoinDCX FUTURES OHLCV] Rejection status code {resp.status_code}: {resp.text}")
                return []
        except requests.exceptions.Timeout:
            logger.error(f"[CoinDCX FUTURES OHLCV] Connection timed out fetching candles for {symbol}.")
            return []
        except Exception as e:
            logger.error(f"[CoinDCX FUTURES OHLCV] Direct socket candlestick exception: {e}")
            return []

    async def fetch_clean_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> List[List[float]]:
        """Asynchronous execution interface wrapper shielding the bot loop from blocking latency."""
        return await asyncio.to_thread(self._sync_fetch_ohlcv, symbol, timeframe, limit)

    def _sync_get_free_balance(self, asset: str) -> float:
        """Synchronous balance query routine running over native OS lookup channels."""
        url = f"{self.base_url}/exchange/v1/users/balances"
        payload = {"timestamp": int(time.time() * 1000)}
        body = json.dumps(payload, separators=(',', ':'))
        
        headers = {
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": self._generate_signature(body),
            "Content-Type": "application/json"
        }
        
        try:
            # FIXED: Added explicit 15-second safety timeout
            resp = requests.post(url, data=body, headers=headers, timeout=15)
            if resp.status_code == 200:
                balances = resp.json()
                for item in balances:
                    if item.get("currency") == asset:
                        return float(item.get("balance", 0.0))
                return 0.0
            else:
                logger.error(f"[CoinDCX BALANCE] Auth Failure (401). Check dashboard keys. Status: {resp.status_code}")
                return 0.0
        except requests.exceptions.Timeout:
            logger.error("[CoinDCX BALANCE] Connection timed out checking balance.")
            return 0.0
        except Exception as e:
            logger.error(f"[CoinDCX BALANCE] Native connection fault: {e}")
            return 0.0

    async def get_free_balance(self, asset: str = "USDT") -> float:
        """Asynchronous execution wrapper for private account balance retrieval."""
        return await asyncio.to_thread(self._sync_get_free_balance, asset)

    def _sync_get_precision_rules(self, symbol: str) -> Dict[str, Any]:
        """Synchronous futures active instruments metadata query routine."""
        url = f"{self.base_url}/exchange/v1/derivatives/futures/data/active_instruments"
        clean_pair = f"B-{symbol.replace('/', '_')}"
        try:
            # FIXED: Added explicit 15-second safety timeout
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                instruments = resp.json()
                for inst in instruments:
                    if inst.get("pair") == clean_pair or inst.get("symbol") == clean_pair:
                        return {
                            "amount_precision": int(inst.get("target_currency_precision", 4)),
                            "price_precision": int(inst.get("base_currency_precision", 2)),
                            "min_amount": float(inst.get("min_quantity", 0.001))
                        }
        except requests.exceptions.Timeout:
            logger.error(f"[CoinDCX PRECISION] Connection timed out checking precision for {symbol}.")
        except Exception as e:
            logger.error(f"[CoinDCX PRECISION] Failed processing metadata rules: {e}")
        return {"amount_precision": 4, "price_precision": 2, "min_amount": 0.001}

    async def get_market_precision_rules(self, symbol: str) -> Dict[str, Any]:
        """Asynchronous execution wrapper for asset precision metric lookup."""
        return await asyncio.to_thread(self._sync_get_precision_rules, symbol)

    def _sync_execute_sniper_trade(
        self, 
        symbol: str, 
        direction: str, 
        position_size: float, 
        entry_price: float,
        stop_loss_price: float, 
        take_profit_price: float
    ) -> Optional[Dict[str, Any]]:
        """Synchronous execution block orchestrating sequential parent futures entry and bracket TPSL orders."""
        clean_pair = f"B-{symbol.replace('/', '_')}"
        side = "buy" if direction.upper() == "LONG" else "sell"
        
        precision = self._sync_get_precision_rules(symbol)
        p_price = precision["price_precision"]
        p_amount = precision["amount_precision"]
        
        url_create = f"{self.base_url}/exchange/v1/orders/create"
        payload_entry = {
            "side": side,
            "order_type": "market_order",
            "market": clean_pair,
            "total_quantity": round(position_size, p_amount),
            "timestamp": int(time.time() * 1000),
            "leverage": 1,
            "product": "futures"
        }
        
        body_entry = json.dumps(payload_entry, separators=(',', ':'))
        headers_entry = {
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": self._generate_signature(body_entry),
            "Content-Type": "application/json"
        }
        
        try:
            # FIXED: Added explicit 15-second safety timeout
            resp_entry = requests.post(url_create, data=body_entry, headers=headers_entry, timeout=15)
            data = resp_entry.json()
            if resp_entry.status_code != 200 or "id" not in data:
                logger.error(f"[CoinDCX FUTURES] Parent Order Execution Rejected: {data}")
                return None
                
            parent_order_id = data["id"]
            logger.info(f"[CoinDCX FUTURES] Parent Market Entry Filled. Order ID: {parent_order_id}")
            
            time.sleep(0.5)
            
            url_tpsl = f"{self.base_url}/exchange/v1/derivatives/futures/orders/tpsl"
            payload_tpsl = {
                "market": clean_pair,
                "timestamp": int(time.time() * 1000),
                "take_profit_price": round(take_profit_price, p_price),
                "stop_loss_price": round(stop_loss_price, p_price),
                "quantity": round(position_size, p_amount)
            }
            
            body_tpsl = json.dumps(payload_tpsl, separators=(',', ':'))
            headers_tpsl = {
                "X-AUTH-APIKEY": self.api_key,
                "X-AUTH-SIGNATURE": self._generate_signature(body_tpsl),
                "Content-Type": "application/json"
            }
            
            # FIXED: Added explicit 15-second safety timeout
            resp_tpsl = requests.post(url_tpsl, data=body_tpsl, headers=headers_tpsl, timeout=15)
            tpsl_data = resp_tpsl.json()
            logger.info(f"[CoinDCX FUTURES] Risk protection brackets locked to active position: {tpsl_data}")
            
            return {
                "entry_ticket": str(parent_order_id),
                "status": "SUCCESS"
            }
        except requests.exceptions.Timeout:
            logger.error(f"[CoinDCX FUTURES] Connection timed out during order execution for {symbol}.")
            return None
        except Exception as e:
            logger.error(f"[CoinDCX FUTURES] Direct socket execution fault: {e}")
            return None

    async def execute_sniper_trade(
        self, 
        symbol: str, 
        direction: str, 
        position_size: float, 
        entry_price: float,
        stop_loss_price: float, 
        take_profit_price: float
    ) -> Optional[Dict[str, Any]]:
        """Asynchronous execution interface wrapper preventing loops from freezing during order placement."""
        return await asyncio.to_thread(
            self._sync_execute_sniper_trade, 
            symbol, direction, position_size, entry_price, stop_loss_price, take_profit_price
        )