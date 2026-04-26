# AI Stock Agent MVP

This project implements a staged MVP from `Macro_Variable_Rolling_Candidate_Pool_System_Blueprint_v4.2.md`.

Current scope includes:
- pool workflow orchestration with LangGraph
- SQLite persistence for pool, audit, archive, post-mortem, and web research records
- pluggable hard-data providers: `mock`, `file`, `http`, `tushare`, `akshare`, `alltick`
- web research providers: `mock`, `file`, `http`, `akshare`
- multi-market demo universes for `US`, `CN`, and `HK`
- file or `akshare` based initial universe generation for `CN` and `HK`
- web evidence to trigger-event synthesis

## Quick Start

```bash
python -m ai_stock_agent.cli bootstrap-demo
python -m ai_stock_agent.cli show-universes
python -m ai_stock_agent.cli run-mvp --mode periodic_review --data-config config/data_sources.json
python -m ai_stock_agent.cli show-pool
python -m ai_stock_agent.cli show-context
python -m ai_stock_agent.cli show-postmortem
python -m ai_stock_agent.cli --db data/generated_cn.db build-universe --market cn --builder-config config/universe_builder.cn.file.json --universe-id cn_generated_live --pool-id cn_generated_live_pool
```

## Codespaces GUI

The lightweight workbench is served by the `serve-gui` command. In Codespaces, bind it to `0.0.0.0` so the forwarded port can reach the server:

```bash
python -m ai_stock_agent.cli --db data/agent.db serve-gui --host 0.0.0.0 --port 8765
```

This repo includes a `.devcontainer` setup that installs the package, seeds `data/agent.db`, starts the dashboard on port `8765`, and asks Codespaces to forward that port automatically.

Useful dashboard URLs:
- `/` or `/dashboard`: browser workbench
- `/health`: JSON health check
- `/api/snapshot`: dashboard data endpoint

## Data Provider Modes

- `mock`: built-in synthetic data for development and workflow validation
- `file`: load local JSON snapshots
- `http`: generic JSON API adapter
- `tushare`: optional Tushare adapter for A-share / HK structured data
- `akshare`: optional AKShare adapter for free public market breadth and quote snapshots
- `alltick`: optional AllTick adapter for HK quote-style data

## Recommended CN / HK Starting Point

If you do not have stable finance APIs yet:

```bash
python -m ai_stock_agent.cli run-mvp --mode periodic_review --universe-id cn_macro_ai --pool-id cn_macro_ai_pool --data-config config/data_sources.free_ah.json --web-config config/web_research.free_ah.json
python -m ai_stock_agent.cli run-mvp --mode periodic_review --universe-id hk_macro_ai --pool-id hk_macro_ai_pool --data-config config/data_sources.free_ah.json --web-config config/web_research.free_ah.json
```

That config keeps `macro_prices` and `sector_signals` on `mock`, while routing:
- `market_breadth` to `akshare`
- `filing_metrics` to `akshare`
- web evidence to `akshare` first, then local file examples

## Recommended CN Hybrid Starting Point

If you already have a Tushare token and want a more stable CN research setup:

```bash
python -m ai_stock_agent.cli run-mvp --mode periodic_review --universe-id cn_macro_ai --pool-id cn_macro_ai_pool --data-config config/data_sources.hybrid_cn.json --web-config config/web_research.hybrid_cn.json
```

That config routes:
- `filing_metrics` to `tushare` first, then `akshare`, then `mock`
- `market_breadth` to `akshare` first, then `mock`
- web evidence to `akshare` first, then local file examples

For generated CN / HK universes backed by live sources:

```bash
python -m ai_stock_agent.cli --db data/generated_cn_live.db build-universe --market cn --builder-config config/universe_builder.cn.hybrid.json --universe-id cn_generated_live --pool-id cn_generated_live_pool
python -m ai_stock_agent.cli --db data/generated_hk_live.db build-universe --market hk --builder-config config/universe_builder.hk.hybrid.json --universe-id hk_generated_live --pool-id hk_generated_live_pool
```

The hybrid builder prefers `tushare` for structured identifiers and valuation fields, then uses `akshare` to enrich quote-style liquidity fields when available.

## Vendor Templates

- `config/data_sources.tushare.template.json`
- `config/data_sources.alltick.template.json`
- `config/data_sources.http.template.json`
- `config/data_sources.hybrid_cn.json`
- `config/web_research.akshare.json`
- `config/web_research.free_ah.json`
- `config/web_research.hybrid_cn.json`
- `config/universe_builder.cn.file.json`
- `config/universe_builder.cn.hybrid.json`
- `config/universe_builder.hk.file.json`
- `config/universe_builder.hk.hybrid.json`

Environment variables used by the templates:
- `TUSHARE_TOKEN`
- `ALLTICK_API_KEY`

These vendor adapters are optional. If the package or token is missing, the provider returns a degraded payload and the workflow can fall back to the next configured source.

## Useful Commands

```bash
python -m ai_stock_agent.cli show-data-sources --data-config config/data_sources.free_ah.json
python -m ai_stock_agent.cli show-pool --pool-id cn_macro_ai_pool
python -m ai_stock_agent.cli show-pool --pool-id hk_macro_ai_pool
python -m ai_stock_agent.cli show-web-sources --web-config config/web_research.json
python -m ai_stock_agent.cli show-research
python -m ai_stock_agent.cli manual-override --entity-id smic_cn --pool-id cn_macro_ai_pool --field thesis_status --value fragile --reason "manual review"
python -m ai_stock_agent.cli --db data/builder_cn.db build-universe --market cn --builder-config config/universe_builder.cn.file.json --universe-id cn_generated_live --pool-id cn_generated_live_pool
python -m ai_stock_agent.cli --db data/builder_cn.db run-mvp --mode event_driven_refresh --universe-id cn_generated_live --pool-id cn_generated_live_pool --event news --ticker 688981.SH --data-config config/data_sources.free_ah.json --web-config config/web_research.free_ah.json
```
