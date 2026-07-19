"""
Module: config_engine
Description: Dynamic configuration configurations for CoinDCX execution parameters.
"""

from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # CoinDCX Configuration Keys
    COINDCX_API_KEY: str = Field(default="mock_key")
    COINDCX_API_SECRET: str = Field(default="mock_secret")
    USE_TESTNET: bool = Field(default=True)
    
    # Absolute Risk Guardrails (1% Rule)
    RISK_PER_TRADE_PCT: float = Field(default=2.0, description="Risk exactly 2% of account balance per trade")
    MAX_OPEN_POSITIONS: int = Field(default=5)
    MIN_SCORE_THRESHOLD: float = Field(default=100.0)

    # Telegram Notification Routing
    TELEGRAM_ENABLED: bool = Field(default=False)
    TELEGRAM_TOKEN: Optional[str] = Field(default=None)
    TELEGRAM_CHAT_ID: Optional[str] = Field(default=None)

    @field_validator("RISK_PER_TRADE_PCT")
    @classmethod
    def validate_risk(cls, value: float) -> float:
        if not 0.0 < value <= 100.0:
            raise ValueError("Risk allocation percentage must be strictly between 0 and 100.")
        return value / 100.0  # Convert 2% to 0.02

settings = AppSettings()