from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .knowledge_base import (
    build_topic_version_diff,
    default_knowledge_policy,
    ingest_knowledge_payload,
    prepare_notebooklm_documents,
    query_knowledge_slices,
    refresh_slice_statuses,
    resolve_document_version,
)
from .storage import SQLiteStateStore
from .universe_builder import build_initial_universe_bundle
from .workflow import apply_manual_override, bootstrap_demo_data, run_mvp_workflow


def _ensure_seeded(db_path: str) -> SQLiteStateStore:
    store = SQLiteStateStore(db_path)
    if not store.list_universes():
        bootstrap_demo_data(db_path)
    return store


def _persist_knowledge_payloads(store: SQLiteStateStore, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for payload in payloads:
        document, slices, collections = ingest_knowledge_payload(payload)
        document, slices, collections, superseded_documents, retired_slices, retired_collections = resolve_document_version(
            document=document,
            slices=slices,
            collections=collections,
            existing_documents=store.list_knowledge_documents(),
            existing_slices=store.list_knowledge_slices(),
            existing_collections=store.list_knowledge_collections(),
        )
        for item in superseded_documents:
            store.save_knowledge_document(item)
        for item in retired_slices:
            store.save_knowledge_slice(item)
        for item in retired_collections:
            store.save_knowledge_collection(item)
        if slices or collections:
            store.save_knowledge_document(document)
            for item in slices:
                store.save_knowledge_slice(item)
            for item in collections:
                store.save_knowledge_collection(item)
        if not store.list_knowledge_policies():
            store.save_knowledge_policy(default_knowledge_policy())
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
    return records


def prepare_notebooklm_batch(text: str, region: str, published_at: str | None = None) -> dict[str, Any]:
    return {
        "documents": prepare_notebooklm_documents(
            text=text,
            region=region,
            published_at=published_at,
        )
    }


def ingest_knowledge_batch(db_path: str, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    store = _ensure_seeded(db_path)
    return _persist_knowledge_payloads(store, payloads)


def refresh_knowledge_state(db_path: str) -> dict[str, int]:
    store = _ensure_seeded(db_path)
    refreshed = refresh_slice_statuses(store.list_knowledge_slices())
    for item in refreshed:
        store.save_knowledge_slice(item)
    return {
        "active": sum(1 for item in refreshed if item.status == "active"),
        "stale": sum(1 for item in refreshed if item.status == "stale"),
        "retired": sum(1 for item in refreshed if item.status == "retired"),
        "total": len(refreshed),
    }


def bootstrap_demo_action(db_path: str) -> dict[str, Any]:
    bootstrap_demo_data(db_path)
    store = SQLiteStateStore(db_path)
    return {
        "universes": len(store.list_universes()),
        "pools": len(store.list_pools()),
        "companies": len(store.list_companies()),
    }


def build_universe_action(db_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    universe, pool, companies = build_initial_universe_bundle(
        market=payload.get("market", "cn"),
        universe_id=payload.get("universe_id"),
        pool_id=payload.get("pool_id"),
        config_path=payload.get("builder_config"),
    )
    store = SQLiteStateStore(db_path)
    store.save_universe(universe)
    store.save_pool(pool)
    for company in companies:
        store.save_company(company)
    return {
        "universe": universe.model_dump(mode="json"),
        "pool": pool.model_dump(mode="json"),
        "company_count": len(companies),
    }


def execute_manual_override_action(db_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    record = apply_manual_override(
        db_path=db_path,
        entity_id=payload["entity_id"],
        override_field=payload["field"],
        new_value=payload["value"],
        reason=payload["reason"],
        operator=payload.get("operator", "gui_operator"),
        pool_id=payload.get("pool_id"),
    )
    return record.model_dump(mode="json")


def _build_topic_tree(documents: list[Any], slices: list[Any], market: str, topic_key: str | None) -> list[dict[str, Any]]:
    filtered_documents = [item for item in documents if item.region == market]
    if topic_key:
        filtered_documents = [item for item in filtered_documents if item.topic_key == topic_key]

    slice_counts: dict[str, int] = {}
    for item in slices:
        slice_counts[item.document_id] = slice_counts.get(item.document_id, 0) + 1

    grouped: dict[str, list[Any]] = {}
    for document in filtered_documents:
        key = document.topic_key or f"document::{document.document_id}"
        grouped.setdefault(key, []).append(document)

    topic_tree: list[dict[str, Any]] = []
    for key, items in grouped.items():
        ordered = sorted(items, key=lambda item: (item.version, item.created_at), reverse=True)
        latest = ordered[0]
        topic_tree.append(
            {
                "topic_key": key,
                "title": latest.title,
                "latest_version": latest.version,
                "latest_status": latest.status,
                "versions": [
                    {
                        "document_id": item.document_id,
                        "title": item.title,
                        "version": item.version,
                        "status": item.status,
                        "published_at": item.published_at,
                        "created_at": item.created_at,
                        "slice_count": slice_counts.get(item.document_id, 0),
                    }
                    for item in ordered
                ],
            }
        )
    topic_tree.sort(key=lambda item: (item["latest_status"] != "active", -item["latest_version"], item["topic_key"]))
    return topic_tree[:20]


def _flatten_official_status(records: list[Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for record in sorted(records, key=lambda item: item.created_at, reverse=True):
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
    return flattened[:20]


def build_dashboard_snapshot(
    db_path: str,
    *,
    market: str = "cn",
    pool_id: str | None = None,
    topic_key: str | None = None,
    from_version: int | None = None,
    to_version: int | None = None,
    layer: str | None = None,
    status: str = "all",
    theme: str = "",
    entity_id: str | None = None,
) -> dict[str, Any]:
    store = _ensure_seeded(db_path)
    slice_status = None if status == "all" else status
    documents = store.list_knowledge_documents()
    slices = store.list_knowledge_slices()
    collections = store.list_knowledge_collections()
    pools = store.list_pools()
    selected_pool = next((item for item in pools if item.pool_id == pool_id), None) if pool_id else None
    if selected_pool is None:
        selected_pool = next((item for item in pools if item.market == market), pools[0] if pools else None)

    knowledge_slices = query_knowledge_slices(
        slices=slices,
        market=market,
        macro_theme=theme,
        layer=layer,
        topic_key=topic_key,
        status=slice_status,
        limit=24,
    )
    filtered_documents = [
        item
        for item in sorted(
            documents,
            key=lambda item: (item.topic_key or "", item.version, item.created_at),
            reverse=True,
        )
        if item.region == market and (topic_key is None or item.topic_key == topic_key)
    ][:24]
    filtered_collections = [
        item
        for item in sorted(collections, key=lambda item: item.updated_at, reverse=True)
        if item.region == market and (topic_key is None or item.topic_key == topic_key)
    ][:16]
    contexts = sorted(store.list_knowledge_contexts(), key=lambda item: item.created_at, reverse=True)[:10]
    market_contexts = sorted(store.list_market_contexts(), key=lambda item: item.as_of, reverse=True)[:10]
    runs = sorted(store.list_runs(), key=lambda item: item.run_id, reverse=True)[:12]
    research_records = sorted(store.list_web_research_records(), key=lambda item: item.created_at, reverse=True)[:12]
    evidence_summaries = sorted(
        store.list_evidence_fusion_summaries(),
        key=lambda item: item.created_at,
        reverse=True,
    )[:16]
    triggers = sorted(store.list_trigger_events(), key=lambda item: item.event_time, reverse=True)[:16]
    overrides = sorted(store.list_manual_overrides(), key=lambda item: item.created_at, reverse=True)[:12]
    audits = sorted(store.list_audit_records(), key=lambda item: item.merged_at, reverse=True)[:12]
    history_folds = sorted(store.list_company_history_folds(), key=lambda item: item.created_at, reverse=True)[:12]
    universes = store.list_universes()
    universe_profiles = {
        entity.entity_id: entity
        for universe in universes
        for entity in universe.eligible_entities
    }
    companies = store.list_companies()
    evidence_by_entity = {
        item.entity_id: item
        for item in evidence_summaries
        if item.entity_id and item.scope == "entity"
    }
    pool_companies: list[dict[str, Any]] = []
    if selected_pool is not None:
        membership_order: list[tuple[str, str]] = []
        for section, ids in (
            ("core", selected_pool.current_pool_members),
            ("shadow", selected_pool.shadow_watch_members),
            ("archived", selected_pool.archived_members),
        ):
            for member_id in ids:
                membership_order.append((member_id, section))
        companies_by_id = {item.entity_id: item for item in companies}
        for member_id, section in membership_order:
            company = companies_by_id.get(member_id)
            if company is None:
                continue
            profile = universe_profiles.get(member_id)
            evidence = evidence_by_entity.get(member_id)
            pool_companies.append(
                {
                    "entity_id": company.entity_id,
                    "ticker": company.ticker,
                    "company_name": company.company_name,
                    "section": section,
                    "current_bucket": company.current_bucket,
                    "current_route": company.current_route,
                    "thesis_status": company.thesis_status,
                    "retention_priority_score": company.retention_priority_score,
                    "current_quality_score": company.current_quality_score,
                    "trajectory_score": company.trajectory_score,
                    "sector": profile.sector if profile else None,
                    "market_cap": profile.market_cap if profile else None,
                    "website": profile.website if profile else None,
                    "historical_tags": company.historical_tags,
                    "recent_thesis_summaries": company.recent_thesis_summaries,
                    "evidence_label": evidence.confidence_label if evidence else None,
                    "evidence_confidence": evidence.overall_confidence if evidence else None,
                    "official_evidence_count": evidence.official_evidence_count if evidence else 0,
                }
            )
    selected_entity_id = entity_id or (pool_companies[0]["entity_id"] if pool_companies else None)
    selected_company = next((item for item in companies if item.entity_id == selected_entity_id), None)
    selected_profile = universe_profiles.get(selected_entity_id) if selected_entity_id else None
    company_detail = None
    if selected_company is not None:
        company_detail = {
            "company": selected_company.model_dump(mode="json"),
            "profile": selected_profile.model_dump(mode="json") if selected_profile else None,
            "evidence": [
                item.model_dump(mode="json")
                for item in evidence_summaries
                if item.entity_id == selected_entity_id
            ],
            "research": [
                item.model_dump(mode="json")
                for item in research_records
                if item.entity_id == selected_entity_id
            ],
            "history_folds": [
                item.model_dump(mode="json")
                for item in history_folds
                if item.entity_id == selected_entity_id
            ],
        }
    topic_diff = None
    if topic_key:
        topic_diff = build_topic_version_diff(
            documents=documents,
            slices=slices,
            topic_key=topic_key,
            region=market,
            from_version=from_version,
            to_version=to_version,
        )
    return {
        "db_path": db_path,
        "market": market,
        "selected_pool_id": selected_pool.pool_id if selected_pool else None,
        "selected_entity_id": selected_entity_id,
        "selected_topic_key": topic_key,
        "selected_from_version": from_version,
        "selected_to_version": to_version,
        "universes": [item.model_dump(mode="json") for item in universes],
        "pools": [item.model_dump(mode="json") for item in pools],
        "selected_pool": selected_pool.model_dump(mode="json") if selected_pool else None,
        "pool_summary": {
            "core": len(selected_pool.current_pool_members) if selected_pool else 0,
            "shadow": len(selected_pool.shadow_watch_members) if selected_pool else 0,
            "archived": len(selected_pool.archived_members) if selected_pool else 0,
            "total": (
                len(selected_pool.current_pool_members)
                + len(selected_pool.shadow_watch_members)
                + len(selected_pool.archived_members)
            )
            if selected_pool
            else 0,
        },
        "pool_companies": pool_companies,
        "company_detail": company_detail,
        "runs": [item.model_dump(mode="json") for item in runs],
        "latest_run": runs[0].model_dump(mode="json") if runs else None,
        "triggers": [item.model_dump(mode="json") for item in triggers],
        "manual_overrides": [item.model_dump(mode="json") for item in overrides],
        "audits": [item.model_dump(mode="json") for item in audits],
        "market_contexts": [item.model_dump(mode="json") for item in market_contexts],
        "knowledge_documents": [item.model_dump(mode="json") for item in filtered_documents],
        "knowledge_slices": [item.model_dump(mode="json") for item in knowledge_slices],
        "knowledge_collections": [item.model_dump(mode="json") for item in filtered_collections],
        "knowledge_contexts": [item.model_dump(mode="json") for item in contexts],
        "topic_tree": _build_topic_tree(documents, slices, market, topic_key),
        "topic_diff": topic_diff,
        "web_research": [item.model_dump(mode="json") for item in research_records],
        "evidence_summaries": [item.model_dump(mode="json") for item in evidence_summaries],
        "official_status": _flatten_official_status(research_records),
        "knowledge_policies": [item.model_dump(mode="json") for item in store.list_knowledge_policies()],
    }


def execute_workflow_action(db_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    result = run_mvp_workflow(
        db_path=db_path,
        data_config_path=payload.get("data_config_path"),
        web_config_path=payload.get("web_config_path"),
        universe_id=payload.get("universe_id") or "cn_macro_ai",
        pool_id=payload.get("pool_id") or "cn_macro_ai_pool",
        run_mode=payload.get("run_mode") or "periodic_review",
        macro_theme=payload.get("macro_theme") or "China AI policy resilience",
        incoming_events=payload.get("incoming_events") or [],
    )
    return {
        "run_state": result["run_state"].model_dump(mode="json"),
        "pool": result["pool"].model_dump(mode="json"),
        "merge_notes": result.get("merge_notes", []),
    }


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AI Stock Agent Workbench</title>
  <style>
    :root {
      --bg: #f4efe6;
      --panel: #fffaf2;
      --panel-2: #fdf7ec;
      --ink: #14213d;
      --muted: #6b7280;
      --line: #d7c8ae;
      --accent: #0f766e;
      --accent-2: #155e75;
      --accent-soft: #d7f0ec;
      --warn: #92400e;
      --warn-soft: #fef3c7;
      --ok: #166534;
      --ok-soft: #dcfce7;
      --mono: "IBM Plex Mono", "Consolas", monospace;
      --sans: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--sans);
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.14), transparent 32%),
        radial-gradient(circle at top right, rgba(146,64,14,0.10), transparent 28%),
        linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
    }
    .shell {
      width: min(1380px, calc(100vw - 32px));
      margin: 24px auto 48px;
    }
    .hero {
      padding: 24px 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: linear-gradient(135deg, rgba(255,250,242,0.96), rgba(244,239,230,0.92));
      box-shadow: 0 12px 36px rgba(20,33,61,0.08);
    }
    .hero h1 {
      margin: 0 0 8px;
      font-size: 34px;
      letter-spacing: -0.04em;
    }
    .hero p {
      margin: 0;
      max-width: 780px;
      color: var(--muted);
      line-height: 1.6;
    }
    .grid {
      display: grid;
      grid-template-columns: 390px minmax(0, 1fr);
      gap: 18px;
      margin-top: 18px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(20,33,61,0.05);
    }
    .panel h2 {
      margin: 0 0 12px;
      font-size: 18px;
    }
    .stack { display: grid; gap: 14px; }
    label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 6px; }
    input, textarea, select, button {
      width: 100%;
      font: inherit;
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 11px 12px;
      background: #fff;
    }
    textarea { min-height: 120px; resize: vertical; }
    button {
      cursor: pointer;
      border: none;
      color: white;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      font-weight: 600;
    }
    button.secondary {
      background: linear-gradient(135deg, #a16207, var(--warn));
    }
    button.ghost {
      background: linear-gradient(135deg, #475467, #344054);
    }
    .row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .row-3 { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .content { display: grid; gap: 18px; }
    .cards { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .grid-3 { display: grid; grid-template-columns: 1.2fr 1fr 1fr; gap: 18px; }
    .stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .stat {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: linear-gradient(180deg, rgba(255,255,255,0.84), rgba(253,247,236,0.92));
    }
    .stat strong {
      display: block;
      font-size: 24px;
      margin-bottom: 6px;
    }
    .stat span {
      color: var(--muted);
      font-size: 13px;
    }
    .list {
      display: grid;
      gap: 10px;
      max-height: 260px;
      overflow: auto;
      padding-right: 4px;
    }
    .list.tall { max-height: 460px; }
    .item {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255,255,255,0.75);
    }
    .item strong { display: block; margin-bottom: 6px; }
    .item button { margin-top: 10px; padding: 8px 10px; font-size: 12px; }
    .meta {
      font-family: var(--mono);
      font-size: 12px;
      color: var(--muted);
      white-space: pre-wrap;
      line-height: 1.55;
    }
    .subhead {
      margin: 0 0 10px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .pill {
      display: inline-block;
      margin-right: 6px;
      margin-bottom: 6px;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: var(--accent-soft);
      color: var(--accent);
    }
    .pill.warn {
      background: var(--warn-soft);
      color: var(--warn);
    }
    .pill.ok {
      background: var(--ok-soft);
      color: var(--ok);
    }
    .status {
      margin-top: 12px;
      padding: 12px;
      border-radius: 16px;
      background: var(--warn-soft);
      color: var(--warn);
      min-height: 54px;
      white-space: pre-wrap;
    }
    .result-box {
      padding: 14px;
      border-radius: 18px;
      border: 1px solid #334155;
      background: #18212f;
      color: #f8fafc;
      font-family: var(--mono);
      font-size: 12px;
      overflow: auto;
      max-height: 220px;
      white-space: pre-wrap;
    }
    pre {
      margin: 0;
      padding: 14px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #1f2937;
      color: #f9fafb;
      font-family: var(--mono);
      font-size: 12px;
      overflow: auto;
    }
    @media (max-width: 980px) {
      .grid, .cards, .row, .row-3, .grid-3, .stats { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>AI Stock Agent Console</h1>
      <p>在浏览器里直接跑工作流、整理 NotebookLM 摘要、查看知识库版本树。这个前端是给高频交互准备的，不替代 Codex，但会把手工操作降很多。</p>
    </section>
    <div class="grid">
      <aside class="stack">
        <section class="panel">
          <h2>Run Workflow</h2>
          <div class="stack">
            <div class="row">
              <div><label>Universe</label><select id="runUniverseId"></select></div>
              <div><label>Pool</label><select id="runPoolId"></select></div>
            </div>
            <div class="row">
              <div><label>Mode</label><select id="runMode"><option>periodic_review</option><option>event_driven_refresh</option><option>initial_build</option><option>recovery_replay</option></select></div>
              <div><label>Event Type</label><select id="eventType"><option value="">none</option><option>macro</option><option>news</option><option>earnings</option><option>guidance</option><option>price_break</option><option>manual</option></select></div>
            </div>
            <div class="row">
              <div><label>Ticker</label><input id="eventTicker" value="688981.SH"></div>
              <div><label>Theme</label><input id="runTheme" value="China AI policy resilience"></div>
            </div>
            <div><label>Data Config</label><input id="dataConfigPath" placeholder="config/data_sources.hybrid_cn.json"></div>
            <div><label>Web Config</label><input id="webConfigPath" placeholder="config/web_research.hybrid_cn.json"></div>
            <button onclick="runWorkflow()">Run Workflow</button>
          </div>
        </section>
        <section class="panel">
          <h2>Action Center</h2>
          <div class="stack">
            <button class="ghost" onclick="seedDemo()">Bootstrap Demo Data</button>
            <button class="secondary" onclick="refreshKnowledge()">Refresh Knowledge Lifecycle</button>
          </div>
        </section>
        <section class="panel">
          <h2>Build Universe</h2>
          <div class="stack">
            <div class="row">
              <div><label>Market</label><select id="buildMarket"><option value="cn">cn</option><option value="hk">hk</option></select></div>
              <div><label>Builder Config</label><input id="builderConfig" placeholder="config/universe_builder.cn.hybrid.json"></div>
            </div>
            <div class="row">
              <div><label>Universe ID</label><input id="buildUniverseId" placeholder="cn_generated_live"></div>
              <div><label>Pool ID</label><input id="buildPoolId" placeholder="cn_generated_live_pool"></div>
            </div>
            <button onclick="buildUniverse()">Build Universe</button>
          </div>
        </section>
        <section class="panel">
          <h2>Manual Override</h2>
          <div class="stack">
            <div class="row">
              <div><label>Entity</label><input id="overrideEntityId" placeholder="smic_cn or 688981.SH"></div>
              <div><label>Pool</label><select id="overridePoolId"></select></div>
            </div>
            <div class="row">
              <div><label>Field</label><select id="overrideField"><option value="current_bucket">current_bucket</option><option value="thesis_status">thesis_status</option></select></div>
              <div><label>Value</label><input id="overrideValue" placeholder="shadow_watch"></div>
            </div>
            <div><label>Reason</label><input id="overrideReason" value="GUI override"></div>
            <div><label>Operator</label><input id="overrideOperator" value="gui_operator"></div>
            <button class="secondary" onclick="applyManualOverride()">Apply Override</button>
          </div>
        </section>
        <section class="panel">
          <h2>NotebookLM Prep</h2>
          <div class="stack">
            <div class="row">
              <div><label>Region</label><input id="prepRegion" value="cn"></div>
              <div><label>Published At</label><input id="prepPublishedAt" value="2026-03-14T09:00:00Z"></div>
            </div>
            <div><label>Cleaned Summary</label><textarea id="prepText">China policy support and credit stabilization are improving the macro backdrop.

Current market consensus is crowded around AI leaders and much of the optimism is priced in.

My investment principle is to wait for a better margin of safety when valuation is stretched.</textarea></div>
            <button class="ghost" onclick="prepNotebooklm()">Prepare JSON</button>
            <button onclick="ingestPrepared()">Ingest Prepared JSON</button>
          </div>
        </section>
        <section class="panel">
          <h2>Knowledge Filters</h2>
          <div class="stack">
            <div class="row">
              <div><label>Market</label><input id="filterMarket" value="cn"></div>
              <div><label>Pool</label><select id="filterPoolId"></select></div>
            </div>
            <div class="row">
              <div><label>Layer</label><select id="filterLayer"><option value="">all</option><option>macro</option><option>consensus</option><option>principle</option></select></div>
              <div><label>Status</label><select id="filterStatus"><option>all</option><option>active</option><option>stale</option><option>retired</option></select></div>
            </div>
            <div class="row">
              <div><label>Topic Key</label><input id="filterTopicKey" placeholder="china_ai_view"></div>
              <div><label>Entity</label><input id="filterEntityId" placeholder="smic_cn"></div>
            </div>
            <div><label>Theme</label><input id="filterTheme" value="China AI policy resilience"></div>
            <button onclick="loadSnapshot()">Refresh View</button>
          </div>
        </section>
        <div class="status" id="statusBox">Ready.</div>
        <div class="result-box" id="actionResultBox">{}</div>
      </aside>
      <main class="content">
        <section class="stats" id="poolStats"></section>
        <section class="panel">
          <h2>Prepared JSON</h2>
          <pre id="preparedJson">{}</pre>
        </section>
        <section class="cards">
          <section class="panel">
            <h2>Pool Companies</h2>
            <div class="list tall" id="poolCompaniesList"></div>
          </section>
          <section class="panel">
            <h2>Company Detail</h2>
            <div id="companyDetail"></div>
          </section>
        </section>
        <section class="grid-3">
          <section class="panel">
            <h2>Topic Version Tree</h2>
            <div class="list tall" id="topicTreeList"></div>
          </section>
          <section class="panel">
            <h2>Evidence</h2>
            <div class="list" id="evidenceList"></div>
          </section>
          <section class="panel">
            <h2>Official Source Status</h2>
            <div class="list" id="officialStatusList"></div>
          </section>
        </section>
        <section class="panel">
          <h2>Topic Version Diff</h2>
          <div class="stack">
            <div class="row">
              <div><label>From Version</label><select id="diffFromVersion"></select></div>
              <div><label>To Version</label><select id="diffToVersion"></select></div>
            </div>
            <div class="row">
              <button class="secondary" onclick="loadSelectedDiff()">Compare Selected Versions</button>
              <button class="ghost" onclick="resetTopicDiff()">Latest Vs Previous</button>
            </div>
            <div id="topicDiff"></div>
          </div>
        </section>
        <section class="cards">
          <section class="panel">
            <h2>Runs</h2>
            <div class="list" id="runsList"></div>
          </section>
          <section class="panel">
            <h2>Market And Knowledge Context</h2>
            <div class="list" id="contextsList"></div>
          </section>
          <section class="panel">
            <h2>Knowledge Documents</h2>
            <div class="list" id="documentsList"></div>
          </section>
          <section class="panel">
            <h2>Knowledge Slices</h2>
            <div class="list" id="slicesList"></div>
          </section>
          <section class="panel">
            <h2>Web Research And Triggers</h2>
            <div class="list" id="researchList"></div>
          </section>
          <section class="panel">
            <h2>Overrides And Audit</h2>
            <div class="list" id="opsList"></div>
          </section>
        </section>
      </main>
    </div>
  </div>
  <script>
    let preparedPayload = {documents: []};
    let activeDiffTopicKey = '';

    function escapeHtml(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function setStatus(message) {
      document.getElementById('statusBox').textContent = message;
    }

    function setActionResult(payload) {
      document.getElementById('actionResultBox').textContent = JSON.stringify(payload, null, 2);
    }

    function renderItems(id, items, builder) {
      const root = document.getElementById(id);
      root.innerHTML = '';
      if (!items || !items.length) {
        root.innerHTML = '<div class="item"><strong>No data</strong><div class="meta">Nothing matched the current filter.</div></div>';
        return;
      }
      (items || []).forEach(item => {
        const div = document.createElement('div');
        div.className = 'item';
        div.innerHTML = builder(item);
        root.appendChild(div);
      });
    }

    function fillSelect(id, items, getValue, getLabel, preferredValue) {
      const select = document.getElementById(id);
      const current = preferredValue ?? select.value;
      select.innerHTML = items.map(item => {
        const value = getValue(item);
        const label = getLabel(item);
        return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
      }).join('');
      if ([...select.options].some(option => option.value === current)) {
        select.value = current;
      }
    }

    function applySnapshotToSelectors(data) {
      fillSelect('filterPoolId', data.pools || [], item => item.pool_id, item => `${item.pool_id} (${item.market})`, data.selected_pool_id || '');
      fillSelect('runPoolId', data.pools || [], item => item.pool_id, item => `${item.pool_id} (${item.market})`, document.getElementById('runPoolId').value || data.selected_pool_id || '');
      fillSelect('overridePoolId', data.pools || [], item => item.pool_id, item => `${item.pool_id} (${item.market})`, document.getElementById('overridePoolId').value || data.selected_pool_id || '');
      fillSelect('runUniverseId', data.universes || [], item => item.universe_id, item => `${item.universe_id} (${item.market})`, document.getElementById('runUniverseId').value || '');
    }

    function clearDiffVersionSelectors() {
      document.getElementById('diffFromVersion').innerHTML = '<option value="">auto</option>';
      document.getElementById('diffToVersion').innerHTML = '<option value="">auto</option>';
    }

    function syncDiffVersionSelectors(data) {
      const topicKey = document.getElementById('filterTopicKey').value;
      const fromSelect = document.getElementById('diffFromVersion');
      const toSelect = document.getElementById('diffToVersion');
      if (!topicKey) {
        activeDiffTopicKey = '';
        clearDiffVersionSelectors();
        return;
      }
      const topic = (data.topic_tree || []).find(item => item.topic_key === topicKey);
      const versions = topic?.versions || [];
      if (!versions.length) {
        activeDiffTopicKey = topicKey;
        clearDiffVersionSelectors();
        return;
      }

      const diff = data.topic_diff || {};
      const diffStats = diff.stats || {};
      const previousTopicKey = activeDiffTopicKey;
      activeDiffTopicKey = topicKey;
      const currentFrom = previousTopicKey === topicKey ? fromSelect.value : '';
      const currentTo = previousTopicKey === topicKey ? toSelect.value : '';
      const defaultTo = String(diffStats.to_version ?? versions[0].version);
      const defaultFrom = String(diffStats.from_version ?? (versions[1]?.version ?? versions[0].version));
      const options = ['<option value="">auto</option>'].concat(
        versions.map(item => `<option value="${escapeHtml(item.version)}">v${escapeHtml(item.version)} ${escapeHtml(item.status)}</option>`)
      );
      fromSelect.innerHTML = options.join('');
      toSelect.innerHTML = options.join('');
      fromSelect.value = [...fromSelect.options].some(option => option.value === currentFrom)
        ? currentFrom
        : ([...fromSelect.options].some(option => option.value === defaultFrom) ? defaultFrom : '');
      toSelect.value = [...toSelect.options].some(option => option.value === currentTo)
        ? currentTo
        : ([...toSelect.options].some(option => option.value === defaultTo) ? defaultTo : '');
    }

    async function api(path, options) {
      const response = await fetch(path, options);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return response.json();
    }

    function renderPoolStats(summary) {
      const root = document.getElementById('poolStats');
      const items = [
        ['Core', summary?.core ?? 0],
        ['Shadow', summary?.shadow ?? 0],
        ['Archived', summary?.archived ?? 0],
        ['Total', summary?.total ?? 0],
      ];
      root.innerHTML = items.map(([label, value]) => `
        <div class="stat">
          <strong>${escapeHtml(value)}</strong>
          <span>${escapeHtml(label)}</span>
        </div>
      `).join('');
    }

    function renderCompanyDetail(detail) {
      const root = document.getElementById('companyDetail');
      if (!detail) {
        root.innerHTML = '<div class="item"><strong>No company selected</strong><div class="meta">Pick a pool member to inspect company details, evidence, and history.</div></div>';
        return;
      }
      const company = detail.company || {};
      const profile = detail.profile || {};
      const evidence = detail.evidence || [];
      const research = detail.research || [];
      const folds = detail.history_folds || [];
      root.innerHTML = `
        <div class="item">
          <strong>${escapeHtml(company.company_name)} (${escapeHtml(company.ticker)})</strong>
          <div class="pill">${escapeHtml(company.current_bucket)}</div>
          <div class="pill">${escapeHtml(company.thesis_status)}</div>
          <div class="pill">${escapeHtml(company.current_route)}</div>
          <div class="meta">sector=${escapeHtml(profile.sector || 'n/a')}
market_cap=${escapeHtml(profile.market_cap ?? 'n/a')}
website=${escapeHtml(profile.website || 'n/a')}
retention=${escapeHtml(company.retention_priority_score ?? 'n/a')}
quality=${escapeHtml(company.current_quality_score ?? 'n/a')}
trajectory=${escapeHtml(company.trajectory_score ?? 'n/a')}</div>
        </div>
        <p class="subhead">Recent Thesis</p>
        <div class="list">${(company.recent_thesis_summaries || []).slice(0, 3).map(item => `<div class="item"><div class="meta">${escapeHtml(item)}</div></div>`).join('') || '<div class="item"><div class="meta">No recent thesis summaries.</div></div>'}</div>
        <p class="subhead">Evidence</p>
        <div class="list">${evidence.slice(0, 3).map(item => `<div class="item"><strong>${escapeHtml(item.confidence_label)}</strong><div class="pill ${item.official_evidence_count ? 'ok' : ''}">official=${escapeHtml(item.official_evidence_count)}</div><div class="meta">${escapeHtml(item.summary)}</div></div>`).join('') || '<div class="item"><div class="meta">No evidence summary yet.</div></div>'}</div>
        <p class="subhead">Research</p>
        <div class="list">${research.slice(0, 3).map(item => `<div class="item"><strong>${escapeHtml(item.provider_id)}</strong><div class="meta">${escapeHtml(item.summary)}</div></div>`).join('') || '<div class="item"><div class="meta">No research records yet.</div></div>'}</div>
        <p class="subhead">History</p>
        <div class="list">${folds.slice(0, 3).map(item => `<div class="item"><strong>${escapeHtml(item.bucket)}</strong><div class="meta">${escapeHtml(item.archived_summary)}</div></div>`).join('') || '<div class="item"><div class="meta">No history folds yet.</div></div>'}</div>
      `;
    }

    function renderTopicDiff(diff) {
      const root = document.getElementById('topicDiff');
      if (!diff) {
        root.innerHTML = '<div class="item"><strong>No diff selected</strong><div class="meta">Focus a topic with at least two versions to compare the latest knowledge update against the previous version.</div></div>';
        return;
      }
      const stats = diff.stats || {};
      const fromDocument = diff.from_document || {};
      const toDocument = diff.to_document || {};
      const takeaways = diff.takeaways || [];
      const documentChanges = diff.document_changes || [];
      const addedSlices = diff.added_slices || [];
      const removedSlices = diff.removed_slices || [];
      const changedSlices = diff.changed_slices || [];
      root.innerHTML = `
        <div class="item">
          <strong>${escapeHtml(diff.title)} (${escapeHtml(diff.topic_key)})</strong>
          <div class="pill">v${escapeHtml(stats.from_version)} -> v${escapeHtml(stats.to_version)}</div>
          <div class="pill ${(stats.changed || stats.added || stats.removed) ? 'warn' : 'ok'}">delta=${escapeHtml((stats.changed ?? 0) + (stats.added ?? 0) + (stats.removed ?? 0))}</div>
          <div class="meta">${escapeHtml(diff.summary)}</div>
        </div>
        <p class="subhead">Analyst Readout</p>
        <div class="list">${takeaways.map(item => `<div class="item"><div class="meta">${escapeHtml(item)}</div></div>`).join('') || '<div class="item"><div class="meta">No automated interpretation yet.</div></div>'}</div>
        <div class="cards">
          <div class="item">
            <strong>From v${escapeHtml(stats.from_version)}</strong>
            <div class="pill ${fromDocument.status === 'active' ? 'ok' : 'warn'}">${escapeHtml(fromDocument.status || 'n/a')}</div>
            <div class="meta">published=${escapeHtml(fromDocument.published_at || 'n/a')}
slices=${escapeHtml(stats.from_slice_count ?? 0)}
summary=${escapeHtml(fromDocument.summary || '')}</div>
          </div>
          <div class="item">
            <strong>To v${escapeHtml(stats.to_version)}</strong>
            <div class="pill ${toDocument.status === 'active' ? 'ok' : 'warn'}">${escapeHtml(toDocument.status || 'n/a')}</div>
            <div class="meta">published=${escapeHtml(toDocument.published_at || 'n/a')}
slices=${escapeHtml(stats.to_slice_count ?? 0)}
summary=${escapeHtml(toDocument.summary || '')}</div>
          </div>
        </div>
        <p class="subhead">Document Changes</p>
        <div class="list">${documentChanges.map(item => `<div class="item"><strong>${escapeHtml(item.field)}</strong><div class="meta">${escapeHtml(item.from ?? 'n/a')} -> ${escapeHtml(item.to ?? 'n/a')}</div></div>`).join('') || '<div class="item"><div class="meta">No top-level document fields changed.</div></div>'}</div>
        <p class="subhead">Added Slices</p>
        <div class="list">${addedSlices.map(item => `<div class="item"><strong>${escapeHtml(item.title)}</strong><div class="pill ok">${escapeHtml(item.layer)}</div><div class="meta">${escapeHtml(item.claim)}</div></div>`).join('') || '<div class="item"><div class="meta">No slices added.</div></div>'}</div>
        <p class="subhead">Removed Slices</p>
        <div class="list">${removedSlices.map(item => `<div class="item"><strong>${escapeHtml(item.title)}</strong><div class="pill warn">${escapeHtml(item.layer)}</div><div class="meta">${escapeHtml(item.claim)}</div></div>`).join('') || '<div class="item"><div class="meta">No slices removed.</div></div>'}</div>
        <p class="subhead">Changed Slices</p>
        <div class="list">${changedSlices.map(item => `<div class="item"><strong>${escapeHtml(item.title)}</strong><div class="pill">${escapeHtml(item.layer)}</div><div class="meta">${item.changes.map(change => `${escapeHtml(change.field)}: ${escapeHtml(change.from ?? 'n/a')} -> ${escapeHtml(change.to ?? 'n/a')}`).join('\\n')}</div></div>`).join('') || '<div class="item"><div class="meta">No matched slices changed in place.</div></div>'}</div>
      `;
    }

    async function loadSnapshot() {
      const params = new URLSearchParams({
        market: document.getElementById('filterMarket').value,
        status: document.getElementById('filterStatus').value,
        theme: document.getElementById('filterTheme').value,
      });
      const poolId = document.getElementById('filterPoolId').value;
      const entityId = document.getElementById('filterEntityId').value;
      const layer = document.getElementById('filterLayer').value;
      const topicKey = document.getElementById('filterTopicKey').value;
      const fromVersion = document.getElementById('diffFromVersion').value;
      const toVersion = document.getElementById('diffToVersion').value;
      if (poolId) params.set('pool_id', poolId);
      if (entityId) params.set('entity_id', entityId);
      if (layer) params.set('layer', layer);
      if (topicKey) {
        params.set('topic_key', topicKey);
        if (fromVersion) params.set('from_version', fromVersion);
        if (toVersion) params.set('to_version', toVersion);
      }
      const data = await api('/api/snapshot?' + params.toString());

      document.querySelector('.hero h1').textContent = 'AI Stock Agent Workbench';
      document.querySelector('.hero p').textContent = 'Run the workflow, manage the knowledge base, inspect pool members, review evidence, and track topic versions from one local browser workbench.';
      applySnapshotToSelectors(data);
      syncDiffVersionSelectors(data);
      renderPoolStats(data.pool_summary);
      renderCompanyDetail(data.company_detail);
      renderTopicDiff(data.topic_diff);

      renderItems('poolCompaniesList', data.pool_companies, item => `
        <strong>${escapeHtml(item.company_name)} (${escapeHtml(item.ticker)})</strong>
        <div class="pill">${escapeHtml(item.section)}</div>
        <div class="pill">${escapeHtml(item.current_bucket)}</div>
        <div class="pill">${escapeHtml(item.thesis_status)}</div>
        ${item.evidence_label ? `<div class="pill ok">${escapeHtml(item.evidence_label)} ${escapeHtml(item.evidence_confidence)}</div>` : ''}
        <div class="meta">route=${escapeHtml(item.current_route)}
sector=${escapeHtml(item.sector || 'n/a')}
retention=${escapeHtml(item.retention_priority_score)}
official=${escapeHtml(item.official_evidence_count || 0)}</div>
        <button class="ghost" onclick="selectEntity('${escapeHtml(item.entity_id)}')">Inspect</button>
      `);
      renderItems('topicTreeList', data.topic_tree, item => `
        <strong>${escapeHtml(item.title)}</strong>
        <div class="pill">${escapeHtml(item.topic_key)}</div>
        <div class="pill ${item.latest_status === 'active' ? 'ok' : 'warn'}">${escapeHtml(item.latest_status)}</div>
        <div class="meta">${item.versions.map(version => `v${version.version} ${version.status} slices=${version.slice_count}`).join('\\n')}</div>
        <button class="ghost" onclick="focusTopic('${escapeHtml(item.topic_key)}')">Focus Topic</button>
        <button class="secondary" onclick="compareTopic('${escapeHtml(item.topic_key)}')">Compare Latest</button>
      `);
      renderItems('evidenceList', data.evidence_summaries, item => `
        <strong>${escapeHtml(item.entity_id || item.scope)}</strong>
        <div class="pill">${escapeHtml(item.confidence_label)}</div>
        <div class="pill ${item.official_evidence_count ? 'ok' : ''}">official=${escapeHtml(item.official_evidence_count)}</div>
        <div class="meta">${escapeHtml(item.summary)}</div>
      `);
      renderItems('officialStatusList', data.official_status, item => `
        <strong>${escapeHtml(item.target)}</strong>
        <div class="pill ${item.status === 'ok' ? 'ok' : 'warn'}">${escapeHtml(item.status)}</div>
        <div class="meta">provider=${escapeHtml(item.provider_id)}
items=${escapeHtml(item.item_count)}
detail=${escapeHtml(item.detail || '')}</div>
      `);
      renderItems('runsList', data.runs, item => `
        <strong>${escapeHtml(item.run_mode)} / ${escapeHtml(item.run_status)}</strong>
        <div class="pill">${escapeHtml(item.market)}</div>
        <div class="meta">${escapeHtml(item.run_id)}
${escapeHtml(item.macro_theme)}</div>
      `);
      renderItems('contextsList', [...(data.market_contexts || []), ...(data.knowledge_contexts || [])].slice(0, 12), item => `
        <strong>${escapeHtml(item.macro_theme || item.regime || 'context')}</strong>
        ${item.regime ? `<div class="pill">${escapeHtml(item.regime)}</div>` : ''}
        ${item.policy_id ? `<div class="pill ok">${escapeHtml(item.policy_id)}</div>` : ''}
        <div class="meta">${escapeHtml(item.context_summary || item.overall_summary || '')}</div>
      `);
      renderItems('documentsList', data.knowledge_documents, item => `
        <strong>${escapeHtml(item.title)}</strong>
        <div class="pill">${escapeHtml(item.topic_key || 'na')}</div>
        <div class="pill">v${escapeHtml(item.version)}</div>
        <div class="pill ${item.status === 'active' ? 'ok' : 'warn'}">${escapeHtml(item.status)}</div>
        <div class="meta">${escapeHtml(item.summary)}</div>
      `);
      renderItems('slicesList', data.knowledge_slices, item => `
        <strong>${escapeHtml(item.title)}</strong>
        <div class="pill">${escapeHtml(item.layer)}</div>
        <div class="pill">${escapeHtml(item.topic_key || 'na')}</div>
        <div class="pill ${item.status === 'active' ? 'ok' : 'warn'}">${escapeHtml(item.status)}</div>
        <div class="meta">${escapeHtml(item.claim)}</div>
      `);
      const researchAndTriggers = [
        ...(data.web_research || []).map(item => ({kind: 'research', payload: item})),
        ...(data.triggers || []).map(item => ({kind: 'trigger', payload: item}))
      ].slice(0, 14);
      renderItems('researchList', researchAndTriggers, item => {
        if (item.kind === 'research') {
          const payload = item.payload;
          return `
            <strong>${escapeHtml(payload.provider_id)}</strong>
            <div class="pill">${escapeHtml(payload.entity_id || 'market')}</div>
            <div class="meta">${escapeHtml(payload.summary)}</div>
          `;
        }
        const payload = item.payload;
        return `
          <strong>${escapeHtml(payload.event_type)}</strong>
          <div class="pill">${escapeHtml(payload.wake_scope)}</div>
          <div class="meta">entities=${escapeHtml((payload.impacted_entities || []).join(', '))}
source=${escapeHtml(payload.source_ref)}</div>
        `;
      });
      const ops = [
        ...(data.manual_overrides || []).map(item => ({kind: 'override', payload: item})),
        ...(data.audits || []).map(item => ({kind: 'audit', payload: item}))
      ].slice(0, 14);
      renderItems('opsList', ops, item => {
        if (item.kind === 'override') {
          const payload = item.payload;
          return `
            <strong>${escapeHtml(payload.override_field)}</strong>
            <div class="pill warn">${escapeHtml(payload.target_object_id)}</div>
            <div class="meta">${escapeHtml(payload.old_value)} -> ${escapeHtml(payload.new_value)}
${escapeHtml(payload.reason)}</div>
          `;
        }
        const payload = item.payload;
        return `
          <strong>${escapeHtml(payload.merged_by)}</strong>
          <div class="pill">${escapeHtml(payload.review_id)}</div>
          <div class="meta">${escapeHtml(payload.state_delta_hash)}</div>
        `;
      });
      setStatus('Snapshot refreshed.');
    }

    async function runWorkflow() {
      const eventType = document.getElementById('eventType').value;
      const ticker = document.getElementById('eventTicker').value;
      const payload = {
        universe_id: document.getElementById('runUniverseId').value,
        pool_id: document.getElementById('runPoolId').value,
        run_mode: document.getElementById('runMode').value,
        macro_theme: document.getElementById('runTheme').value,
        data_config_path: document.getElementById('dataConfigPath').value || null,
        web_config_path: document.getElementById('webConfigPath').value || null,
        incoming_events: eventType ? [{event_type: eventType, ticker: ticker, source_ref: 'gui'}] : [],
      };
      const data = await api('/api/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      setActionResult(data);
      setStatus('Workflow finished.');
      await loadSnapshot();
    }

    async function seedDemo() {
      const data = await api('/api/bootstrap-demo', {method: 'POST'});
      setActionResult(data);
      setStatus('Demo data bootstrapped.');
      await loadSnapshot();
    }

    async function buildUniverse() {
      const payload = {
        market: document.getElementById('buildMarket').value,
        universe_id: document.getElementById('buildUniverseId').value || null,
        pool_id: document.getElementById('buildPoolId').value || null,
        builder_config: document.getElementById('builderConfig').value || null,
      };
      const data = await api('/api/build-universe', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      setActionResult(data);
      setStatus('Universe built and persisted.');
      await loadSnapshot();
    }

    async function prepNotebooklm() {
      const payload = {
        text: document.getElementById('prepText').value,
        region: document.getElementById('prepRegion').value,
        published_at: document.getElementById('prepPublishedAt').value,
      };
      preparedPayload = await api('/api/notebooklm-prep', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      document.getElementById('preparedJson').textContent = JSON.stringify(preparedPayload, null, 2);
      setActionResult(preparedPayload);
      setStatus('Prepared NotebookLM payload.');
    }

    async function ingestPrepared() {
      const data = await api('/api/knowledge/batch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(preparedPayload),
      });
      setActionResult(data);
      setStatus('Prepared knowledge batch ingested.');
      await loadSnapshot();
    }

    async function refreshKnowledge() {
      const data = await api('/api/knowledge/refresh', {method: 'POST'});
      setActionResult(data);
      setStatus('Knowledge lifecycle refreshed.');
      await loadSnapshot();
    }

    async function applyManualOverride() {
      const payload = {
        entity_id: document.getElementById('overrideEntityId').value,
        pool_id: document.getElementById('overridePoolId').value || null,
        field: document.getElementById('overrideField').value,
        value: document.getElementById('overrideValue').value,
        reason: document.getElementById('overrideReason').value,
        operator: document.getElementById('overrideOperator').value,
      };
      const data = await api('/api/manual-override', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      setActionResult(data);
      setStatus('Manual override persisted.');
      await loadSnapshot();
    }

    function focusTopic(topicKey) {
      document.getElementById('filterTopicKey').value = topicKey;
      clearDiffVersionSelectors();
      loadSnapshot().catch(error => setStatus(String(error)));
    }

    function compareTopic(topicKey) {
      document.getElementById('filterTopicKey').value = topicKey;
      clearDiffVersionSelectors();
      loadSnapshot()
        .then(() => setStatus(`Loaded version diff for ${topicKey}.`))
        .catch(error => setStatus(String(error)));
    }

    function loadSelectedDiff() {
      const topicKey = document.getElementById('filterTopicKey').value;
      const fromVersion = document.getElementById('diffFromVersion').value;
      const toVersion = document.getElementById('diffToVersion').value;
      if (!topicKey) {
        setStatus('Focus a topic before comparing versions.');
        return;
      }
      if (fromVersion && toVersion && fromVersion === toVersion) {
        setStatus('Choose two different versions to compare.');
        return;
      }
      if (fromVersion && toVersion && Number(fromVersion) > Number(toVersion)) {
        setStatus('Choose an older "from" version and a newer "to" version.');
        return;
      }
      loadSnapshot()
        .then(() => setStatus(`Loaded custom diff for ${topicKey}.`))
        .catch(error => setStatus(String(error)));
    }

    function resetTopicDiff() {
      clearDiffVersionSelectors();
      loadSnapshot()
        .then(() => setStatus('Loaded latest-vs-previous diff.'))
        .catch(error => setStatus(String(error)));
    }

    function selectEntity(entityId) {
      document.getElementById('filterEntityId').value = entityId;
      document.getElementById('overrideEntityId').value = entityId;
      loadSnapshot().catch(error => setStatus(String(error)));
    }

    clearDiffVersionSelectors();
    loadSnapshot()
      .then(() => prepNotebooklm())
      .catch(error => setStatus(String(error)));
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: str = "data/agent.db"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _json_response(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html_response(self, payload: str) -> None:
        body = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        return json.loads(raw or "{}")

    @staticmethod
    def _read_optional_int(value: str | None) -> int | None:
        if value in {None, ""}:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._html_response(HTML_PAGE)
            return
        if parsed.path == "/api/snapshot":
            params = parse_qs(parsed.query)
            payload = build_dashboard_snapshot(
                self.db_path,
                market=params.get("market", ["cn"])[0],
                pool_id=params.get("pool_id", [None])[0],
                topic_key=params.get("topic_key", [None])[0],
                from_version=self._read_optional_int(params.get("from_version", [None])[0]),
                to_version=self._read_optional_int(params.get("to_version", [None])[0]),
                layer=params.get("layer", [None])[0],
                status=params.get("status", ["all"])[0],
                theme=params.get("theme", [""])[0],
                entity_id=params.get("entity_id", [None])[0],
            )
            self._json_response(payload)
            return
        self._json_response({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/api/run":
                self._json_response(execute_workflow_action(self.db_path, self._read_json()))
                return
            if self.path == "/api/bootstrap-demo":
                self._json_response(bootstrap_demo_action(self.db_path))
                return
            if self.path == "/api/build-universe":
                self._json_response(build_universe_action(self.db_path, self._read_json()))
                return
            if self.path == "/api/manual-override":
                self._json_response(execute_manual_override_action(self.db_path, self._read_json()))
                return
            if self.path == "/api/notebooklm-prep":
                payload = self._read_json()
                self._json_response(
                    prepare_notebooklm_batch(
                        payload.get("text", ""),
                        payload.get("region", "cn"),
                        payload.get("published_at"),
                    )
                )
                return
            if self.path == "/api/knowledge/batch":
                payload = self._read_json()
                documents = payload.get("documents") or []
                self._json_response({"records": ingest_knowledge_batch(self.db_path, documents)})
                return
            if self.path == "/api/knowledge/refresh":
                self._json_response(refresh_knowledge_state(self.db_path))
                return
        except Exception as exc:  # noqa: BLE001
            self._json_response({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._json_response({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)


def serve_dashboard(db_path: str, host: str = "127.0.0.1", port: int = 8765) -> None:
    DashboardHandler.db_path = db_path
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port} using db={Path(db_path)}")
    server.serve_forever()
