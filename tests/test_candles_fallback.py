import asyncio
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import moex_bot


def test_futures_base_code_handles_long_prefixes():
    assert moex_bot._futures_base_code("SBERM6") == "SBER"
    assert moex_bot._futures_base_code("BRN6") == "BR"
    assert moex_bot._futures_base_code("SIU6") == "SI"


def test_tinkoff_headers_include_auth_and_json():
    headers = moex_bot._tinkoff_headers()
    assert headers["Authorization"].startswith("Bearer ")
    assert headers["Content-Type"] == "application/json"


def test_ai_evaluate_news_falls_back_when_ai_disabled(monkeypatch):
    monkeypatch.setattr(moex_bot, "get_ai_enabled", lambda: False)
    monkeypatch.setattr(moex_bot, "get_bot_settings", lambda: {"min_tech_score_confirmed": 61})

    result = asyncio.run(moex_bot.ai_evaluate_news([], "SBER", "банки", "🟩 LONG", 70))

    assert result["filter_status"] == "CONFIRMED"
    assert result["ai_skip_reason"] == "ai_disabled"


def test_fetch_candles_tinkoff_returns_dataframe_on_success(monkeypatch):
    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "candles": [
                    {
                        "time": "2024-01-01T00:00:00Z",
                        "open": {"units": 100, "nano": 0},
                        "high": {"units": 101, "nano": 0},
                        "low": {"units": 99, "nano": 0},
                        "close": {"units": 101, "nano": 0},
                        "volume": 123,
                    }
                ]
            }

    class FakeSession:
        def post(self, *args, **kwargs):
            return FakeResponse()

    async def fake_acquire():
        return None

    monkeypatch.setattr(moex_bot, "_get_http_session", lambda: FakeSession())
    monkeypatch.setattr(moex_bot._tinkoff_rate_limiter, "acquire", fake_acquire)
    monkeypatch.setattr(moex_bot, "_cache", {})

    df = asyncio.run(moex_bot.fetch_candles_tinkoff("BBGTEST", "CANDLE_INTERVAL_DAY", 5))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["close"] == 101.0
