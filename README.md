# Argentina Chain Tracker

Daily macro intelligence pipeline for Argentine markets.

## Week 1 scope

- Pull and store data from 3 public sources:
  - FRED U.S. Treasury yields
  - BCRA reserves and base monetaria
  - DolarHoy FX rates
- Persist daily JSON snapshots in `data/{date}/`
- Append pull logs to `logs/pull_log.jsonl`
- Keep source and analyst registries in `config/`

## Setup

```bash
python -m venv .venv
```

Activate virtual environment:

- Windows PowerShell:

```bash
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment setup

1. Copy `.env.example` to `.env`.
2. Set these required values in `.env`:

```env
FRED_API_KEY=your_key_here
CHAIN_TRACKER_PASSWORD=your_app_password
CHAIN_TRACKER_COOKIE_SECRET=long_random_secret
```

FRED key registration: `https://fred.stlouisfed.org/docs/api/api_key.html`

## Run

```bash
python runner.py
```

Run the web app:

```bash
python app.py
```

Run web smoke tests:

```bash
python scripts/smoke_web.py
```

## Vercel Deployment

This repo includes `vercel.json` for Flask deployment.

Set these environment variables in Vercel:

- `CHAIN_TRACKER_PASSWORD`
- `CHAIN_TRACKER_COOKIE_SECRET`
- `FRED_API_KEY` (if pullers run in that environment)

## Output locations

- Daily data: `data/{date}/`
- Pull logs: `logs/pull_log.jsonl`

## Architecture reference

- Transfer architecture and doctrine context: `../OPUS.txt`
- Macro decision doctrine: `../FINANCE_OPERATING_SYSTEM/`

## Notes

- Week 1 is data ingestion only. No dashboard and no analysis layer.
- If FRED key is missing, the US yields puller returns a graceful error payload.
- Pullers are defensive by design: parse errors return structured errors instead of crashing.
- Secrets are env-only (`.env`), not tracked in `config/app_config.json`.

## Intelligence Sources

This project tracks 33 curated data sources across 5 layers:
- Layer 1 (Global Inputs): 9 sources
- Layer 2 (Transmission): 5 sources
- Layer 3 (Monetary/Fiscal): 6 sources
- Layer 4 (Local Markets): 7 sources
- Layer 5 (Regulatory/Political): 5 sources

Plus 13 tracked analysts with methodology visibility ratings.

Source registry: `config/source_registry.json`  
Analyst registry: `config/analyst_registry.json`  
Competitive benchmark: `config/competitive_benchmark.json`  
Design decisions: `docs/design_decisions.md`
