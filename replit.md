# MOEX Telegram Trading Bot

A Python Telegram bot (~7000 lines) for trading on the Moscow Exchange (MOEX). It fetches market data, performs technical analysis, generates trading signals, and can integrate with AI models (Gemini/Groq) for insights.

## How to run

The bot is configured as the "Start application" workflow. It starts with:

```
python moex_bot.py
```

## Environment variables / Secrets

| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_TOKEN` | **Yes** | Telegram Bot API token from @BotFather |
| `TINKOFF_TOKEN` | No | Tinkoff Invest API token for order execution |
| `GEMINI_API_KEY` | No | Google Gemini API key for AI analysis |
| `GROQ_API_KEY` | No | Groq API key for AI analysis |
| `REDIS_URL` | No | Redis connection URL (e.g. `redis://...`) for persistent storage across restarts |
| `RSSHUB_URL` | No | RSSHub instance URL for news feeds (defaults to `https://rsshub.app`) |

## Stack

- Python 3.12
- python-telegram-bot 20.7
- aiohttp (async HTTP + webhook server on PORT 8080)
- pandas, numpy, pandas-ta (technical analysis)
- redis (optional persistence)
- Google Gemini / Groq (optional AI)

## Source files

- `moex_bot.py` — main bot (active)
- `moex_bot.py111`, `moex_bot.py2222`, etc. — legacy backup versions (not used)

## User preferences

- Keep existing project structure and stack; do not restructure unless asked.
- Tasks will be given one at a time after initial setup.
- **GOLDEN RULE: Never touch original files** (`moex_bot.py` or any other original). Even if explicitly asked to edit the original — always work on a copy. Apply fixes to a copy, push the copy to a separate branch.
