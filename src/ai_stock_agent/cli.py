from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from .data_adapters import load_provider_configs
from .dashboard import serve_dashboard
from .knowledge_base import (
    default_knowledge_policy,
    ingest_knowledge_payload,
    load_pdf_knowledge_payloads,
    load_knowledge_document_payloads,
    load_knowledge_document_payload,
    load_text_payload,
    prepare_notebooklm_documents,
    query_knowledge_slices,
    refresh_slice_statuses,
    resolve_document_version,
)
from .storage import SQLiteStateStore
from .tushare_probe import probe_tushare_access
from .universe_builder import build_initial_universe_bundle
from .workflow import apply_manual_override, bootstrap_demo_data, run_mvp_workflow
from .web_research import load_web_provider_configs


DEFAULT_DB = Path("data/agent.db")
DEFAULT_KNOWLEDGE_LIBRARY_ROOT = Path(r"D:\知识库_投资")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI stock agent MVP CLI")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap-demo", help="Create demo universe and pool state")
    subparsers.add_parser("show-universes", help="Display available demo universes and pools")
    build_universe_parser = subparsers.add_parser("build-universe", help="Build a market universe and seeded pool")
    build_universe_parser.add_argument("--market", required=True, choices=["cn", "hk"])
    build_universe_parser.add_argument("--universe-id")
    build_universe_parser.add_argument("--pool-id")
    build_universe_parser.add_argument("--builder-config", help="Optional universe builder config path")

    run_parser = subparsers.add_parser("run-mvp", help="Run the MVP workflow")
    run_parser.add_argument(
        "--mode",
        default="periodic_review",
        choices=["initial_build", "periodic_review", "event_driven_refresh", "recovery_replay"],
    )
    run_parser.add_argument("--theme", default="AI infrastructure resilience")
    run_parser.add_argument("--universe-id", default="us_macro_ai")
    run_parser.add_argument("--pool-id", default="us_macro_ai_pool")
    run_parser.add_argument("--data-config", help="Optional JSON data provider config path")
    run_parser.add_argument("--web-config", help="Optional JSON web research provider config path")
    run_parser.add_argument(
        "--event",
        choices=["earnings", "macro", "guidance", "price_break", "news", "manual"],
        help="Optional trigger event type",
    )
    run_parser.add_argument("--ticker", help="Ticker for the optional event")

    pool_parser = subparsers.add_parser("show-pool", help="Display current pool state")
    pool_parser.add_argument("--pool-id", default="us_macro_ai_pool")
    latest_run_parser = subparsers.add_parser("show-latest-run", help="Display a concise summary of the latest workflow run")
    latest_run_parser.add_argument("--run-id", help="Optional explicit run id")
    latest_run_parser.add_argument("--limit", type=int, default=10, help="Maximum number of deltas to include")
    run_trace_parser = subparsers.add_parser("show-run-trace", help="Display the recorded step-by-step workflow trace")
    run_trace_parser.add_argument("--run-id", help="Optional explicit run id")
    subparsers.add_parser("show-audit", help="Display audit records")
    subparsers.add_parser("show-context", help="Display theme decomposition and market context snapshots")
    subparsers.add_parser("show-prescreen", help="Display factor registry and challenger prescreen decisions")
    subparsers.add_parser("show-history", help="Display pool snapshots and archive history")
    subparsers.add_parser("show-postmortem", help="Display evaluation metrics and post-mortem reports")
    subparsers.add_parser("probe-tushare", help="Probe current Tushare token permissions")
    data_parser = subparsers.add_parser("show-data-sources", help="Display resolved data provider configs")
    data_parser.add_argument("--data-config", help="Optional JSON data provider config path")
    web_parser = subparsers.add_parser("show-web-sources", help="Display resolved web research provider configs")
    web_parser.add_argument("--web-config", help="Optional JSON web research provider config path")
    subparsers.add_parser("show-research", help="Display stored web research records")
    subparsers.add_parser("show-official-status", help="Display official source collection diagnostics")
    subparsers.add_parser("show-evidence", help="Display stored evidence fusion summaries")
    ingest_parser = subparsers.add_parser("ingest-knowledge", help="Ingest a knowledge article into the curated knowledge base")
    ingest_parser.add_argument("--payload", required=True, help="Path to JSON payload or inline JSON string")
    batch_ingest_parser = subparsers.add_parser("ingest-knowledge-batch", help="Ingest multiple knowledge articles from a JSON array, {documents: []}, or directory")
    batch_ingest_parser.add_argument("--payload", required=True, help="Path to JSON payload array/object, directory, or inline JSON string")
    library_ingest_parser = subparsers.add_parser("ingest-knowledge-library", help="Recursively ingest PDF knowledge documents from a local library root")
    library_ingest_parser.add_argument("--root", default=str(DEFAULT_KNOWLEDGE_LIBRARY_ROOT), help="Knowledge library root directory or a single PDF path")
    library_ingest_parser.add_argument("--region", default="cn")
    library_ingest_parser.add_argument("--source-name", default="investment_library")
    library_ingest_parser.add_argument("--limit", type=int, help="Optional maximum number of PDFs to ingest")
    library_ingest_parser.add_argument("--max-pages", type=int, default=40, help="Maximum pages to extract from each PDF")
    library_ingest_parser.add_argument("--max-chars", type=int, default=30000, help="Maximum characters to extract from each PDF")
    library_ingest_parser.add_argument("--disable-ocr", action="store_true", help="Disable OCR fallback for scanned/image-heavy PDFs")
    library_ingest_parser.add_argument("--ocr-min-chars", type=int, default=240, help="Run OCR when native extraction returns fewer than this many characters")
    show_knowledge_parser = subparsers.add_parser("show-knowledge", help="Display stored knowledge documents, slices, collections, and policy")
    show_knowledge_parser.add_argument("--market", default="cn")
    show_knowledge_parser.add_argument("--layer", choices=["macro", "consensus", "principle"])
    show_knowledge_parser.add_argument("--topic-key")
    show_knowledge_parser.add_argument("--topic-tag", action="append", default=[])
    show_knowledge_parser.add_argument("--entity-tag", action="append", default=[])
    show_knowledge_parser.add_argument("--status", choices=["active", "stale", "retired", "all"], default="all")
    show_knowledge_parser.add_argument("--limit", type=int)
    show_knowledge_parser.add_argument("--theme", default="")
    subparsers.add_parser("show-knowledge-context", help="Display stored workflow knowledge overlay records")
    subparsers.add_parser("refresh-knowledge", help="Refresh knowledge slice lifecycle status based on age and validity")
    prep_parser = subparsers.add_parser("notebooklm-prep", help="Convert NotebookLM-style cleaned text into a batch JSON payload")
    prep_parser.add_argument("--input", required=True, help="Path to UTF-8 text/markdown or inline text")
    prep_parser.add_argument("--region", default="cn")
    prep_parser.add_argument("--published-at")
    gui_parser = subparsers.add_parser("serve-gui", help="Launch a lightweight local web console for the agent")
    gui_parser.add_argument("--host", default="127.0.0.1")
    gui_parser.add_argument("--port", type=int, default=8765)
    override_parser = subparsers.add_parser("manual-override", help="Apply a minimal manual override")
    override_parser.add_argument("--entity-id", required=True)
    override_parser.add_argument("--pool-id")
    override_parser.add_argument("--field", required=True, choices=["current_bucket", "thesis_status"])
    override_parser.add_argument("--value", required=True)
    override_parser.add_argument("--reason", required=True)
    override_parser.add_argument("--operator", default="human_operator")
    return parser


def cmd_bootstrap_demo(db_path: str) -> None:
    bootstrap_demo_data(db_path)
    print(f"Bootstrapped demo data into {db_path}")


def cmd_build_universe(
    db_path: str,
    market: str,
    universe_id: str | None,
    pool_id: str | None,
    builder_config: str | None,
) -> None:
    universe, pool, companies = build_initial_universe_bundle(
        market=market,
        universe_id=universe_id,
        pool_id=pool_id,
        config_path=builder_config,
    )
    store = SQLiteStateStore(db_path)
    store.save_universe(universe)
    store.save_pool(pool)
    for company in companies:
        store.save_company(company)
    print(json.dumps(universe.model_dump(mode="json"), indent=2, ensure_ascii=False))
    print(json.dumps(pool.model_dump(mode="json"), indent=2, ensure_ascii=False))


def cmd_run_mvp(
    db_path: str,
    mode: str,
    theme: str,
    universe_id: str,
    pool_id: str,
    event: str | None,
    ticker: str | None,
    data_config: str | None,
    web_config: str | None,
) -> None:
    incoming_events: list[dict] = []
    if event:
        incoming_events.append(
            {
                "event_type": event,
                "ticker": ticker,
                "source_ref": "cli",
            }
        )
    result = run_mvp_workflow(
        db_path=db_path,
        data_config_path=data_config,
        web_config_path=web_config,
        universe_id=universe_id,
        pool_id=pool_id,
        run_mode=mode,
        macro_theme=theme,
        incoming_events=incoming_events,
    )
    print(json.dumps(result["run_state"].model_dump(mode="json"), indent=2, ensure_ascii=False))
    print(json.dumps(result["pool"].model_dump(mode="json"), indent=2, ensure_ascii=False))


def cmd_show_universes(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    print(json.dumps([item.model_dump(mode="json") for item in store.list_universes()], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in store.list_pools()], indent=2, ensure_ascii=False))


def cmd_show_pool(db_path: str, pool_id: str) -> None:
    store = SQLiteStateStore(db_path)
    pool = store.load_pool(pool_id)
    if pool is None:
        print("Pool not found. Run bootstrap-demo first.")
        return
    pool_company_ids = set(pool.current_pool_members) | set(pool.shadow_watch_members) | set(pool.archived_members)
    companies = [company for company in store.list_companies() if company.entity_id in pool_company_ids]
    print(json.dumps(pool.model_dump(mode="json"), indent=2, ensure_ascii=False))
    print(json.dumps([company.model_dump(mode="json") for company in companies], indent=2, ensure_ascii=False))


def cmd_show_latest_run(db_path: str, run_id: str | None, limit: int) -> None:
    store = SQLiteStateStore(db_path)
    runs = store.list_runs()
    if not runs:
        print("No runs found. Run run-mvp first.")
        return

    selected = store.load_run(run_id) if run_id else None
    if run_id and selected is None:
        print(f"Run not found: {run_id}")
        return
    if selected is None:
        selected = max(
            runs,
            key=lambda item: (
                item.created_at or datetime.min.replace(tzinfo=UTC),
                item.run_id,
            ),
        )

    deltas = selected.decision_output.get("deltas", [])
    ordered_deltas = sorted(
        deltas,
        key=lambda item: item.get("score_snapshot", {}).get("retention_priority_score", 0.0),
        reverse=True,
    )
    research_records = [item for item in store.list_web_research_records() if item.run_id == selected.run_id]
    provider_counts = Counter(item.provider_id for item in research_records)
    latest_context = None
    contexts = store.list_market_contexts()
    if contexts:
        latest_context = max(
            contexts,
            key=lambda item: (
                item.as_of,
                item.context_id,
            ),
        )
    summary = {
        "run_id": selected.run_id,
        "created_at": selected.created_at.isoformat() if selected.created_at else None,
        "run_mode": selected.run_mode,
        "run_status": selected.run_status,
        "market": selected.market,
        "macro_theme": selected.macro_theme,
        "wake_scope": selected.wake_scope,
        "trigger_event_count": len(selected.trigger_event_ids),
        "incumbent_review_count": len(selected.incumbent_review_set),
        "challenger_count": len(selected.challenger_set),
        "delta_count": len(deltas),
        "process_step_count": len(selected.process_trace),
        "web_provider_counts": dict(provider_counts),
        "notes": selected.decision_output.get("notes", []),
        "latest_market_context": (
            {
                "as_of": latest_context.as_of.isoformat(),
                "regime": latest_context.regime,
                "context_status": latest_context.context_status,
                "regime_confidence": latest_context.regime_confidence,
                "context_summary": latest_context.context_summary,
                "source_health": [
                    {
                        "data_domain": item.data_domain,
                        "primary_source": item.primary_source,
                        "backup_source": item.backup_source,
                        "status": item.status,
                        "field_completeness": item.field_completeness,
                        "latency_hours": item.latency_hours,
                        "degradation_path": item.degradation_path,
                    }
                    for item in latest_context.source_health
                ],
            }
            if latest_context
            else None
        ),
        "top_deltas": [
            {
                "entity_id": item.get("entity_id"),
                "route": item.get("route"),
                "from_bucket": item.get("from_bucket"),
                "to_bucket": item.get("to_bucket"),
                "from_thesis_status": item.get("from_thesis_status"),
                "to_thesis_status": item.get("to_thesis_status"),
                "current_quality_score": item.get("score_snapshot", {}).get("current_quality_score"),
                "trajectory_score": item.get("score_snapshot", {}).get("trajectory_score"),
                "retention_priority_score": item.get("score_snapshot", {}).get("retention_priority_score"),
            }
            for item in ordered_deltas[:limit]
        ],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


def cmd_show_run_trace(db_path: str, run_id: str | None) -> None:
    store = SQLiteStateStore(db_path)
    runs = store.list_runs()
    if not runs:
        print("No runs found. Run run-mvp first.")
        return

    selected = store.load_run(run_id) if run_id else None
    if run_id and selected is None:
        print(f"Run not found: {run_id}")
        return
    if selected is None:
        selected = max(
            runs,
            key=lambda item: (
                item.created_at or datetime.min.replace(tzinfo=UTC),
                item.run_id,
            ),
        )

    print(
        json.dumps(
            {
                "run_id": selected.run_id,
                "created_at": selected.created_at.isoformat() if selected.created_at else None,
                "run_mode": selected.run_mode,
                "run_status": selected.run_status,
                "market": selected.market,
                "process_trace": selected.process_trace,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


def cmd_show_audit(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    audits = store.list_audit_records()
    recoveries = store.list_recovery_records()
    print(json.dumps([audit.model_dump(mode="json") for audit in audits], indent=2, ensure_ascii=False))
    print(json.dumps([record.model_dump(mode="json") for record in recoveries], indent=2, ensure_ascii=False))


def cmd_show_context(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    decompositions = store.list_theme_decompositions()
    contexts = store.list_market_contexts()
    health = store.list_source_health()
    print(json.dumps([item.model_dump(mode="json") for item in decompositions], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in contexts], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in health], indent=2, ensure_ascii=False))


def cmd_show_prescreen(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    factors = store.list_factor_registry_items()
    decisions = store.list_prescreen_decisions()
    print(json.dumps([item.model_dump(mode="json") for item in factors], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in decisions], indent=2, ensure_ascii=False))


def cmd_show_history(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    snapshots = store.list_pool_snapshots()
    folds = store.list_company_history_folds()
    archives = store.list_archive_version_records()
    print(json.dumps([item.model_dump(mode="json") for item in snapshots], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in folds], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in archives], indent=2, ensure_ascii=False))


def cmd_show_postmortem(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    metrics = store.list_evaluation_metrics()
    reports = store.list_post_mortem_reports()
    print(json.dumps([item.model_dump(mode="json") for item in metrics], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in reports], indent=2, ensure_ascii=False))


def cmd_probe_tushare() -> None:
    print(json.dumps(probe_tushare_access(), indent=2, ensure_ascii=False))


def cmd_show_data_sources(data_config: str | None) -> None:
    providers = load_provider_configs(data_config)
    print(json.dumps([item.model_dump(mode="json") for item in providers], indent=2, ensure_ascii=False))


def cmd_show_web_sources(web_config: str | None) -> None:
    providers = load_web_provider_configs(web_config)
    print(json.dumps([item.model_dump(mode="json") for item in providers], indent=2, ensure_ascii=False))


def cmd_show_research(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    records = store.list_web_research_records()
    print(json.dumps([item.model_dump(mode="json") for item in records], indent=2, ensure_ascii=False))


def cmd_show_official_status(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    records = store.list_web_research_records()
    flattened: list[dict] = []
    for record in records:
        if not record.provider_id.startswith("web_official"):
            continue
        for diagnostic in record.collection_diagnostics:
            flattened.append(
                {
                    "record_id": record.record_id,
                    "provider_id": record.provider_id,
                    "entity_id": record.entity_id,
                    "query": record.query,
                    **diagnostic.model_dump(mode="json"),
                }
            )
    print(json.dumps(flattened, indent=2, ensure_ascii=False))


def cmd_show_evidence(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    summaries = store.list_evidence_fusion_summaries()
    print(json.dumps([item.model_dump(mode="json") for item in summaries], indent=2, ensure_ascii=False))


def cmd_ingest_knowledge(db_path: str, payload: str) -> None:
    store = SQLiteStateStore(db_path)
    document, slices, collections = ingest_knowledge_payload(load_knowledge_document_payload(payload))
    document, slices, collections = _persist_knowledge_ingest(store, document, slices, collections)
    print(json.dumps(document.model_dump(mode="json"), indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in slices], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in collections], indent=2, ensure_ascii=False))


def _persist_knowledge_ingest(store: SQLiteStateStore, document, slices, collections):
    document, slices, collections, superseded_documents, retired_slices, retired_collections = resolve_document_version(
        document=document,
        slices=slices,
        collections=collections,
        existing_documents=store.list_knowledge_documents(),
        existing_slices=store.list_knowledge_slices(),
        existing_collections=store.list_knowledge_collections(),
    )
    if not slices and not collections:
        return document, slices, collections
    for item in superseded_documents:
        store.save_knowledge_document(item)
    for item in retired_slices:
        store.save_knowledge_slice(item)
    for item in retired_collections:
        store.save_knowledge_collection(item)
    store.save_knowledge_document(document)
    for item in slices:
        store.save_knowledge_slice(item)
    for item in collections:
        store.save_knowledge_collection(item)
    existing_policies = store.list_knowledge_policies()
    if not existing_policies:
        store.save_knowledge_policy(default_knowledge_policy())
    return document, slices, collections


def cmd_ingest_knowledge_batch(db_path: str, payload: str) -> None:
    store = SQLiteStateStore(db_path)
    records: list[dict] = []
    for item in load_knowledge_document_payloads(payload):
        document, slices, collections = ingest_knowledge_payload(item)
        document, slices, collections = _persist_knowledge_ingest(store, document, slices, collections)
        records.append(
            {
                "document_id": document.document_id,
                "title": document.title,
                "topic_key": document.topic_key,
                "version": document.version,
                "slice_count": len(slices),
                "collection_count": len(collections),
            }
        )
    print(json.dumps(records, indent=2, ensure_ascii=False))


def cmd_ingest_knowledge_library(
    db_path: str,
    root: str,
    region: str,
    source_name: str,
    limit: int | None,
    max_pages: int,
    max_chars: int,
    disable_ocr: bool,
    ocr_min_chars: int,
) -> None:
    store = SQLiteStateStore(db_path)
    records: list[dict] = []
    for item in load_pdf_knowledge_payloads(
        root,
        region=region,
        source_name=source_name,
        limit=limit,
        max_pages=max_pages,
        max_chars=max_chars,
        enable_ocr=not disable_ocr,
        ocr_min_chars=ocr_min_chars,
    ):
        document, slices, collections = ingest_knowledge_payload(item)
        document, slices, collections = _persist_knowledge_ingest(store, document, slices, collections)
        records.append(
            {
                "document_id": document.document_id,
                "title": document.title,
                "topic_key": document.topic_key,
                "version": document.version,
                "slice_count": len(slices),
                "collection_count": len(collections),
                "status": "existing" if not slices and not collections else "ingested",
                "relative_path": item.get("relative_path"),
                "source_path": item.get("source_path"),
            }
        )
    print(json.dumps(records, indent=2, ensure_ascii=False))


def cmd_show_knowledge(
    db_path: str,
    market: str,
    layer: str | None,
    topic_key: str | None,
    topic_tags: list[str],
    entity_tags: list[str],
    status: str,
    limit: int | None,
    theme: str,
) -> None:
    store = SQLiteStateStore(db_path)
    documents = store.list_knowledge_documents()
    slices = store.list_knowledge_slices()
    collections = store.list_knowledge_collections()
    policies = store.list_knowledge_policies()
    slice_status = None if status == "all" else status
    filtered_slices = query_knowledge_slices(
        slices=slices,
        market=market,
        macro_theme=theme,
        layer=layer,
        topic_key=topic_key,
        topic_tags=topic_tags,
        entity_tags=entity_tags,
        status=slice_status,
        limit=limit,
    )
    print(json.dumps([item.model_dump(mode="json") for item in documents], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in filtered_slices], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in collections], indent=2, ensure_ascii=False))
    print(json.dumps([item.model_dump(mode="json") for item in policies], indent=2, ensure_ascii=False))


def cmd_show_knowledge_context(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    contexts = store.list_knowledge_contexts()
    print(json.dumps([item.model_dump(mode="json") for item in contexts], indent=2, ensure_ascii=False))


def cmd_refresh_knowledge(db_path: str) -> None:
    store = SQLiteStateStore(db_path)
    refreshed = refresh_slice_statuses(store.list_knowledge_slices())
    for item in refreshed:
        store.save_knowledge_slice(item)
    counts = {
        "active": sum(1 for item in refreshed if item.status == "active"),
        "stale": sum(1 for item in refreshed if item.status == "stale"),
        "retired": sum(1 for item in refreshed if item.status == "retired"),
        "total": len(refreshed),
    }
    print(json.dumps(counts, indent=2, ensure_ascii=False))


def cmd_notebooklm_prep(input_value: str, region: str, published_at: str | None) -> None:
    documents = prepare_notebooklm_documents(
        text=load_text_payload(input_value),
        region=region,
        published_at=published_at,
    )
    print(json.dumps({"documents": documents}, indent=2, ensure_ascii=False))


def cmd_serve_gui(db_path: str, host: str, port: int) -> None:
    serve_dashboard(db_path, host=host, port=port)


def cmd_manual_override(
    db_path: str,
    entity_id: str,
    pool_id: str | None,
    field: str,
    value: str,
    reason: str,
    operator: str,
) -> None:
    record = apply_manual_override(db_path, entity_id, field, value, reason, operator, pool_id=pool_id)
    print(json.dumps(record.model_dump(mode="json"), indent=2, ensure_ascii=False))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "bootstrap-demo":
        cmd_bootstrap_demo(args.db)
    elif args.command == "build-universe":
        cmd_build_universe(args.db, args.market, args.universe_id, args.pool_id, args.builder_config)
    elif args.command == "show-universes":
        cmd_show_universes(args.db)
    elif args.command == "run-mvp":
        cmd_run_mvp(
            args.db,
            args.mode,
            args.theme,
            args.universe_id,
            args.pool_id,
            args.event,
            args.ticker,
            args.data_config,
            args.web_config,
        )
    elif args.command == "show-pool":
        cmd_show_pool(args.db, args.pool_id)
    elif args.command == "show-latest-run":
        cmd_show_latest_run(args.db, args.run_id, args.limit)
    elif args.command == "show-run-trace":
        cmd_show_run_trace(args.db, args.run_id)
    elif args.command == "show-audit":
        cmd_show_audit(args.db)
    elif args.command == "show-context":
        cmd_show_context(args.db)
    elif args.command == "show-prescreen":
        cmd_show_prescreen(args.db)
    elif args.command == "show-history":
        cmd_show_history(args.db)
    elif args.command == "show-postmortem":
        cmd_show_postmortem(args.db)
    elif args.command == "probe-tushare":
        cmd_probe_tushare()
    elif args.command == "show-data-sources":
        cmd_show_data_sources(args.data_config)
    elif args.command == "show-web-sources":
        cmd_show_web_sources(args.web_config)
    elif args.command == "show-research":
        cmd_show_research(args.db)
    elif args.command == "show-official-status":
        cmd_show_official_status(args.db)
    elif args.command == "ingest-knowledge":
        cmd_ingest_knowledge(args.db, args.payload)
    elif args.command == "ingest-knowledge-batch":
        cmd_ingest_knowledge_batch(args.db, args.payload)
    elif args.command == "ingest-knowledge-library":
        cmd_ingest_knowledge_library(
            args.db,
            args.root,
            args.region,
            args.source_name,
            args.limit,
            args.max_pages,
            args.max_chars,
            args.disable_ocr,
            args.ocr_min_chars,
        )
    elif args.command == "show-knowledge":
        cmd_show_knowledge(
            args.db,
            args.market,
            args.layer,
            args.topic_key,
            args.topic_tag,
            args.entity_tag,
            args.status,
            args.limit,
            args.theme,
        )
    elif args.command == "show-knowledge-context":
        cmd_show_knowledge_context(args.db)
    elif args.command == "refresh-knowledge":
        cmd_refresh_knowledge(args.db)
    elif args.command == "notebooklm-prep":
        cmd_notebooklm_prep(args.input, args.region, args.published_at)
    elif args.command == "serve-gui":
        cmd_serve_gui(args.db, args.host, args.port)
    elif args.command == "manual-override":
        cmd_manual_override(args.db, args.entity_id, args.pool_id, args.field, args.value, args.reason, args.operator)
    elif args.command == "show-evidence":
        cmd_show_evidence(args.db)


if __name__ == "__main__":
    main()
