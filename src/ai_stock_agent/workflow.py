from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from .context_engine import build_market_context, decompose_theme
from .data_adapters import build_data_source_manager
from .demo import build_all_demo_sets
from .evidence_fusion import build_entity_evidence_summary, build_market_evidence_summary
from .factor_engine import build_default_factor_registry, prescreen_challenger
from .knowledge_base import build_knowledge_context, default_knowledge_policy
from .models import (
    ArchiveVersionRecord,
    AuditTrailRecord,
    CompanyHistoryFold,
    Bucket,
    CompanyStateRecord,
    CurrentRoute,
    DataSourceHealthRecord,
    EvidenceFusionSummary,
    EntityProfile,
    EvaluationMetricsRecord,
    ExecutionRecoveryRecord,
    FactorRegistryItem,
    KnowledgeContextRecord,
    MarketContextSnapshot,
    ManualOverrideRecord,
    PolicyVersionSet,
    PoolVersionSnapshot,
    PostMortemReport,
    PrescreenDecision,
    ResearchPoolState,
    RunMode,
    RunState,
    RunStatus,
    ScoreCard,
    StateDelta,
    ThemeDecomposition,
    ThesisStatus,
    TriggerEventRecord,
    WakeScope,
    WeightCalibrationPolicy,
    WebResearchRecord,
)
from .postmortem_engine import build_evaluation_metrics, build_post_mortem_report
from .scoring import (
    apply_score_to_company,
    build_prescreen_shadow_scorecard,
    build_frozen_packet,
    compute_scorecard,
    infer_thesis_status,
    make_new_company_state,
    validate_bucket_transition,
    validate_thesis_transition,
)
from .storage import SQLiteStateStore
from .utils import make_id, model_hash
from .web_research import build_web_research_manager, synthesize_trigger_events


class WorkflowState(TypedDict, total=False):
    db_path: str
    data_config_path: str | None
    web_config_path: str | None
    universe_id: str
    pool_id: str
    macro_theme: str
    run_mode: str
    incoming_events: list[dict[str, Any]]
    run_state: RunState
    universe: Any
    pool: Any
    companies: dict[str, CompanyStateRecord]
    entity_map: dict[str, EntityProfile]
    trigger_events: list[TriggerEventRecord]
    web_research_records: list[WebResearchRecord]
    knowledge_context: KnowledgeContextRecord
    theme_decomposition: ThemeDecomposition
    market_context: MarketContextSnapshot
    source_health: list[DataSourceHealthRecord]
    factor_registry: list[FactorRegistryItem]
    prescreen_results: dict[str, PrescreenDecision]
    wake_entities: list[str]
    defender_ids: list[str]
    challenger_ids: list[str]
    scorecards: dict[str, ScoreCard]
    packets: dict[str, Any]
    deltas: list[StateDelta]
    audits: list[AuditTrailRecord]
    recoveries: list[ExecutionRecoveryRecord]
    pool_snapshot: PoolVersionSnapshot
    company_folds: list[CompanyHistoryFold]
    archive_record: ArchiveVersionRecord
    evaluation_metrics: EvaluationMetricsRecord
    post_mortem_report: PostMortemReport
    evidence_summaries: list[EvidenceFusionSummary]
    merge_notes: list[str]


EVIDENCE_PROMOTION_MIN_CONFIDENCE = 0.64
OBSERVATION_ONLY_EVIDENCE_LABELS = {"watch", "fragile"}


def _store(state: WorkflowState) -> SQLiteStateStore:
    return SQLiteStateStore(state["db_path"])


def _data_source_manager(state: WorkflowState):
    return build_data_source_manager(state.get("data_config_path"))


def _web_research_manager(state: WorkflowState):
    return build_web_research_manager(state.get("web_config_path"))


def _ticker_matches(entity_ticker: str, raw_ticker: str | None) -> bool:
    if not raw_ticker:
        return False
    candidates = {
        entity_ticker.upper(),
        entity_ticker.upper().split(".", 1)[0],
        "".join(char for char in entity_ticker if char.isdigit()),
    }
    incoming = raw_ticker.upper()
    incoming_digits = "".join(char for char in incoming if char.isdigit())
    return incoming in candidates or incoming.split(".", 1)[0] in candidates or (
        incoming_digits and incoming_digits in candidates
    )


def _resolve_pool_for_entity(
    store: SQLiteStateStore,
    entity_id: str,
    pool_id: str | None = None,
) -> ResearchPoolState | None:
    if pool_id:
        return store.load_pool(pool_id)
    for pool in store.list_pools():
        if entity_id in pool.current_pool_members or entity_id in pool.shadow_watch_members or entity_id in pool.archived_members:
            return pool
    return None


def bootstrap_demo_data(db_path: str | Path) -> None:
    store = SQLiteStateStore(db_path)
    for universe, pool, companies in build_all_demo_sets():
        store.save_universe(universe)
        store.save_pool(pool)
        for company in companies:
            store.save_company(company)
    for item in build_default_factor_registry():
        store.save_factor_registry_item(item)


def _load_context(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    universe = store.load_universe(state["universe_id"])
    pool = store.load_pool(state["pool_id"])
    if universe is None or pool is None:
        bootstrap_demo_data(state["db_path"])
        universe = store.load_universe(state["universe_id"])
        pool = store.load_pool(state["pool_id"])
    companies = {company.entity_id: company for company in store.list_companies()}
    entity_map = {entity.entity_id: entity for entity in universe.eligible_entities}
    factor_registry = store.list_factor_registry_items()
    if not factor_registry:
        factor_registry = build_default_factor_registry()
        for item in factor_registry:
            store.save_factor_registry_item(item)
    return {
        "universe": universe,
        "pool": pool,
        "companies": companies,
        "entity_map": entity_map,
        "factor_registry": factor_registry,
    }


def _n0_initialize_run(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    context = _load_context(state)
    incoming_events = state.get("incoming_events", [])
    run_mode = RunMode(state["run_mode"])
    run_state = RunState(
        run_id=make_id("run"),
        run_mode=run_mode,
        run_status=RunStatus.RUNNING,
        market=context["universe"].market,
        macro_theme=state.get("macro_theme", "AI infrastructure resilience"),
        trigger_event_ids=[],
        wake_scope=WakeScope.POOL if run_mode != RunMode.EVENT_DRIVEN_REFRESH else WakeScope.ENTITY,
        input_snapshot_hash=model_hash(
            {
                "universe": context["universe"].model_dump(mode="json"),
                "pool": context["pool"].model_dump(mode="json"),
                "events": incoming_events,
            }
        ),
        policy_version_set=PolicyVersionSet(),
        idempotency_key=make_id("idempotency"),
    )
    store.save_run(run_state)
    return context | {"run_state": run_state, "merge_notes": []}


def _trigger_triage(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    entity_map = state["entity_map"]
    raw_events = state.get("incoming_events", [])
    dedup_keys: set[str] = set()
    trigger_events: list[TriggerEventRecord] = []
    wake_entities: set[str] = set()
    for raw in raw_events:
        dedup_key = raw.get("event_deduplication_key") or f"{raw['event_type']}:{raw.get('ticker', 'pool')}"
        if dedup_key in dedup_keys:
            continue
        dedup_keys.add(dedup_key)
        impacted = raw.get("impacted_entities") or [
            entity_id
            for entity_id, entity in entity_map.items()
            if _ticker_matches(entity.ticker, raw.get("ticker"))
        ]
        event = TriggerEventRecord(
            event_id=make_id("evt"),
            event_type=raw["event_type"],
            event_time=raw.get("event_time", datetime.now(UTC)),
            source_type=raw.get("source_type", "system"),
            source_ref=raw.get("source_ref", "cli"),
            event_deduplication_key=dedup_key,
            wake_scope=raw.get("wake_scope", "entity" if impacted else "pool"),
            impacted_entities=impacted,
            trigger_confidence=raw.get("trigger_confidence", 0.8),
            cooldown_group=raw.get("cooldown_group", raw["event_type"]),
            parent_event_id=raw.get("parent_event_id"),
        )
        store.save_trigger_event(event)
        trigger_events.append(event)
        wake_entities.update(impacted)

    if not trigger_events:
        wake_entities.update(state["pool"].current_pool_members)

    run_state = state["run_state"].model_copy(
        update={"trigger_event_ids": [event.event_id for event in trigger_events]}
    )
    store.save_run(run_state)
    return {"trigger_events": trigger_events, "wake_entities": sorted(wake_entities), "run_state": run_state}


def _web_research(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    entity_map = state["entity_map"]
    existing_events = store.list_trigger_events()
    entities = [entity_map[entity_id] for entity_id in state.get("wake_entities", []) if entity_id in entity_map]
    records = _web_research_manager(state).collect(
        run_id=state["run_state"].run_id,
        macro_theme=state["run_state"].macro_theme,
        trigger_events=state.get("trigger_events", []),
        entities=entities,
    )
    for record in records:
        store.save_web_research_record(record)
    synthesized_events = synthesize_trigger_events(
        run_id=state["run_state"].run_id,
        records=records,
        entity_map=entity_map,
        existing_events=existing_events,
    )
    for event in synthesized_events:
        store.save_trigger_event(event)
    notes = list(state.get("merge_notes", []))
    if records:
        notes.append(f"web_research_records={len(records)}")
    if synthesized_events:
        notes.append(f"web_trigger_events={len(synthesized_events)}")
    wake_entities = set(state.get("wake_entities", []))
    for event in synthesized_events:
        wake_entities.update(event.impacted_entities)
    run_state = state["run_state"].model_copy(
        update={
            "trigger_event_ids": [
                *state["run_state"].trigger_event_ids,
                *[event.event_id for event in synthesized_events],
            ]
        }
    )
    store.save_run(run_state)
    return {
        "web_research_records": records,
        "merge_notes": notes,
        "trigger_events": [*state.get("trigger_events", []), *synthesized_events],
        "wake_entities": sorted(wake_entities),
        "run_state": run_state,
    }


def _n1_theme_decomposition(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    decomposition = decompose_theme(state["run_state"].macro_theme, state.get("trigger_events", []))
    if state.get("web_research_records"):
        decomposition = decomposition.model_copy(
            update={
                "summary": f"{decomposition.summary} Web research records: {len(state['web_research_records'])}."
            }
        )
    if state.get("knowledge_context") is not None:
        decomposition = decomposition.model_copy(
            update={
                "summary": f"{decomposition.summary} {state['knowledge_context'].overall_summary}"
            }
        )
    store.save_theme_decomposition(decomposition)
    return {"theme_decomposition": decomposition}


def _knowledge_overlay(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    policies = store.list_knowledge_policies()
    policy = policies[-1] if policies else default_knowledge_policy()
    if not policies:
        store.save_knowledge_policy(policy)
    knowledge_context = build_knowledge_context(
        run_id=state["run_state"].run_id,
        macro_theme=state["run_state"].macro_theme,
        market=state["universe"].market,
        slices=store.list_knowledge_slices(),
        policy=policy,
        wake_entity_ids=state.get("wake_entities", []),
    )
    store.save_knowledge_context(knowledge_context)
    notes = list(state.get("merge_notes", []))
    notes.append(
        "knowledge_overlay="
        f"macro:{knowledge_context.macro_signal.score:.2f},"
        f"consensus:{knowledge_context.consensus_signal.score:.2f},"
        f"principle:{knowledge_context.principle_signal.score:.2f}"
    )
    return {"knowledge_context": knowledge_context, "merge_notes": notes}


def _n2_market_context_loader(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    market_context = build_market_context(
        macro_theme=state["run_state"].macro_theme,
        decomposition=state["theme_decomposition"],
        trigger_events=state.get("trigger_events", []),
        data_source_manager=_data_source_manager(state),
        knowledge_context=state.get("knowledge_context"),
    )
    store.save_market_context(market_context)
    for record in market_context.source_health:
        store.save_source_health(record)
    market_summary = build_market_evidence_summary(
        run_id=state["run_state"].run_id,
        market_context=market_context,
        web_records=state.get("web_research_records", []),
        trigger_events=state.get("trigger_events", []),
    )
    store.save_evidence_fusion_summary(market_summary)
    notes = list(state.get("merge_notes", []))
    notes.append(f"market_evidence={market_summary.confidence_label}:{market_summary.overall_confidence:.2f}")
    return {
        "market_context": market_context,
        "source_health": market_context.source_health,
        "evidence_summaries": [market_summary],
        "merge_notes": notes,
    }


def _n3_incumbent_health_check(state: WorkflowState) -> WorkflowState:
    pool: ResearchPoolState = state["pool"]
    companies = state["companies"]
    market_context = state["market_context"]
    defenders: list[str] = []
    for entity_id in pool.current_pool_members:
        company = companies[entity_id]
        if company.retention_priority_score < 55 or company.staleness_level in {"aging", "stale"}:
            defenders.append(entity_id)
        elif market_context.regime == "risk_off" and company.current_bucket == Bucket.HIGH_BETA_WATCH:
            defenders.append(entity_id)
    if not defenders:
        defenders = list(pool.current_pool_members)
    run_state = state["run_state"].model_copy(update={"incumbent_review_set": defenders})
    _store(state).save_run(run_state)
    return {"defender_ids": defenders, "run_state": run_state}


def _n4_defender_selection(state: WorkflowState) -> WorkflowState:
    return {"defender_ids": state["defender_ids"][:5]}


def _n5_external_challenger_scan(state: WorkflowState) -> WorkflowState:
    pool: ResearchPoolState = state["pool"]
    entity_map = state["entity_map"]
    theme = state["theme_decomposition"]
    context = state["market_context"]
    current_members = set(pool.current_pool_members) | set(pool.shadow_watch_members) | set(pool.archived_members)
    candidates = [entity for entity in entity_map.values() if entity.entity_id not in current_members]
    theme_factors = {factor for item in theme.theme_slices for factor in item.linked_factors}

    def rank_candidate(entity: EntityProfile) -> float:
        overlap = len(theme_factors.intersection(entity.active_factor_exposures))
        regime_bonus = 4 if context.regime == "balanced_growth" and entity.risk_penalty < 45 else 0
        risk_off_penalty = 5 if context.regime == "risk_off" and entity.risk_penalty > 40 else 0
        return (
            entity.valuation_score
            + entity.catalyst_score
            + entity.macro_alignment_score
            + overlap * 4
            + regime_bonus
            - entity.risk_penalty
            - risk_off_penalty
        )

    candidates.sort(
        key=rank_candidate,
        reverse=True,
    )
    challenger_ids = [entity.entity_id for entity in candidates[:5]]
    run_state = state["run_state"].model_copy(update={"challenger_set": challenger_ids})
    _store(state).save_run(run_state)
    return {"challenger_ids": challenger_ids, "run_state": run_state}


def _n6_challenger_prescreen(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    entity_map = state["entity_map"]
    policy = WeightCalibrationPolicy()
    prescreen_results: dict[str, PrescreenDecision] = {}
    passed_challengers: list[str] = []
    merge_notes = list(state.get("merge_notes", []))

    for entity_id in state["challenger_ids"]:
        entity = entity_map[entity_id]
        decision = prescreen_challenger(
            entity=entity,
            theme=state["theme_decomposition"],
            market_context=state["market_context"],
            factor_registry=state["factor_registry"],
            policy=policy,
        )
        prescreen_results[entity_id] = decision
        store.save_prescreen_decision(decision)
        if decision.passed:
            passed_challengers.append(entity_id)
        else:
            merge_notes.append(
                f"{entity_id} prescreen={decision.decision} reasons={','.join(decision.blocked_reasons) or 'none'}"
            )

    run_state = state["run_state"].model_copy(update={"challenger_set": passed_challengers})
    store.save_run(run_state)
    return {
        "prescreen_results": prescreen_results,
        "challenger_ids": passed_challengers,
        "run_state": run_state,
        "merge_notes": merge_notes,
    }


def _n7_deep_scoring(state: WorkflowState) -> WorkflowState:
    policy = WeightCalibrationPolicy()
    entity_map = state["entity_map"]
    companies = state["companies"]
    packets = dict(state.get("packets", {}))
    scorecards: dict[str, ScoreCard] = {}
    evidence_summaries = list(state.get("evidence_summaries", []))
    data_source_manager = _data_source_manager(state)
    store = _store(state)
    watched_ids = list(dict.fromkeys(state["defender_ids"] + state["challenger_ids"]))
    evidence_by_entity: dict[str, EvidenceFusionSummary] = {
        summary.entity_id: summary for summary in evidence_summaries if summary.entity_id
    }
    for entity_id in watched_ids:
        entity = entity_map[entity_id]
        packet = build_frozen_packet(entity, data_source_manager)
        packets[entity_id] = packet
        evidence_summary = build_entity_evidence_summary(
            run_id=state["run_state"].run_id,
            entity_id=entity_id,
            packet=packet,
            web_records=state.get("web_research_records", []),
            trigger_events=state.get("trigger_events", []),
        )
        evidence_by_entity[entity_id] = evidence_summary
        store.save_evidence_fusion_summary(evidence_summary)
        prescreen = state.get("prescreen_results", {}).get(entity_id)
        scorecard = compute_scorecard(
            entity,
            packet,
            policy,
            entity_id in state["challenger_ids"],
            theme=state["theme_decomposition"],
            market_context=state["market_context"],
            prescreen=prescreen,
            evidence_summary=evidence_summary,
            knowledge_context=state.get("knowledge_context"),
        )
        scorecards[entity_id] = scorecard
        existing_company = companies.get(entity_id)
        if existing_company is None:
            route = (
                prescreen.eligible_route
                if prescreen is not None
                else CurrentRoute.CHALLENGER_SCAN if entity_id in state["challenger_ids"] else CurrentRoute.INCUMBENT_REVIEW
            )
            companies[entity_id] = make_new_company_state(entity, Bucket.SHADOW_WATCH, route)
        companies[entity_id] = apply_score_to_company(
            companies[entity_id],
            scorecard,
            packet,
            market_context=state["market_context"],
        )
    merged_summaries = [
        *[summary for summary in evidence_summaries if summary.entity_id is None],
        *[evidence_by_entity[key] for key in sorted(evidence_by_entity)],
    ]
    return {"packets": packets, "scorecards": scorecards, "companies": companies, "evidence_summaries": merged_summaries}


def _make_delta(
    company: CompanyStateRecord,
    scorecard: ScoreCard,
    to_bucket: Bucket,
    to_status: ThesisStatus,
    route: CurrentRoute,
    run_state: RunState,
) -> StateDelta:
    if company.thesis_status == ThesisStatus.FORMING and to_status == ThesisStatus.ACCELERATING:
        to_status = ThesisStatus.VALIDATED
    if company.thesis_status == ThesisStatus.FORMING and to_status == ThesisStatus.BROKEN:
        to_status = ThesisStatus.FRAGILE
    if company.thesis_status == ThesisStatus.FRAGILE and to_status == ThesisStatus.ACCELERATING:
        to_status = ThesisStatus.VALIDATED
    validate_bucket_transition(company.current_bucket, to_bucket)
    validate_thesis_transition(company.thesis_status, to_status)
    payload = {
        "entity_id": company.entity_id,
        "to_bucket": to_bucket,
        "to_status": to_status,
        "scorecard": scorecard.model_dump(mode="json"),
        "run_id": run_state.run_id,
    }
    return StateDelta(
        delta_id=make_id("delta"),
        entity_id=company.entity_id,
        from_bucket=company.current_bucket,
        to_bucket=to_bucket,
        from_thesis_status=company.thesis_status,
        to_thesis_status=to_status,
        route=route,
        rationale="Deterministic score decision under blueprint v4.2",
        score_snapshot=scorecard,
        trigger_event_ids=run_state.trigger_event_ids,
        input_snapshot_hash=run_state.input_snapshot_hash,
        idempotency_key=model_hash(payload),
    )


def _evidence_summary_map(state: WorkflowState) -> dict[str, EvidenceFusionSummary]:
    return {
        summary.entity_id: summary
        for summary in state.get("evidence_summaries", [])
        if summary.entity_id is not None
    }


def _promotion_gate_note(entity_id: str, summary: EvidenceFusionSummary) -> str:
    return (
        f"{entity_id} evidence_gate={summary.confidence_label}:{summary.overall_confidence:.2f} "
        "promotion_blocked"
    )


def _challenger_promotion_allowed(state: WorkflowState, entity_id: str) -> tuple[bool, str | None]:
    summary = _evidence_summary_map(state).get(entity_id)
    if summary is None:
        return False, f"{entity_id} evidence_gate=missing promotion_blocked"
    if (
        summary.confidence_label in OBSERVATION_ONLY_EVIDENCE_LABELS
        or summary.overall_confidence < EVIDENCE_PROMOTION_MIN_CONFIDENCE
    ):
        return False, _promotion_gate_note(entity_id, summary)
    knowledge_context = state.get("knowledge_context")
    entity = state["entity_map"].get(entity_id)
    if knowledge_context is not None and entity is not None:
        if (
            abs(min(0.0, knowledge_context.consensus_signal.score)) >= 0.6
            and knowledge_context.consensus_signal.confidence >= 0.55
            and entity.risk_penalty >= 35
        ):
            return False, f"{entity_id} crowding_gate=high promotion_blocked"
        if (
            "valuation_discipline" in knowledge_context.principle_signal.risk_flags
            and entity.valuation_score < 60
        ):
            return False, f"{entity_id} principle_gate=valuation promotion_blocked"
    return True, None


def _incumbent_upgrade_allowed(
    state: WorkflowState,
    entity_id: str,
    current_bucket: Bucket,
    target_bucket: Bucket,
) -> tuple[Bucket, str | None]:
    if not (current_bucket == Bucket.SECONDARY_CANDIDATES and target_bucket == Bucket.CORE_TRACKING):
        return target_bucket, None
    summary = _evidence_summary_map(state).get(entity_id)
    if summary is None:
        return current_bucket, f"{entity_id} evidence_gate=missing core_upgrade_blocked"
    if (
        summary.confidence_label in OBSERVATION_ONLY_EVIDENCE_LABELS
        or summary.overall_confidence < EVIDENCE_PROMOTION_MIN_CONFIDENCE
    ):
        return current_bucket, f"{entity_id} evidence_gate={summary.confidence_label}:{summary.overall_confidence:.2f} core_upgrade_blocked"
    return target_bucket, None


def _n8_arena(state: WorkflowState) -> WorkflowState:
    defenders = sorted(
        state["defender_ids"],
        key=lambda entity_id: state["scorecards"][entity_id].retention_priority_score,
    )
    challengers = sorted(
        state["challenger_ids"],
        key=lambda entity_id: state["scorecards"][entity_id].retention_priority_score,
        reverse=True,
    )
    notes: list[str] = []
    deltas: list[StateDelta] = []
    for defender_id, challenger_id in zip(defenders, challengers):
        defender_score = state["scorecards"][defender_id]
        challenger_score = state["scorecards"][challenger_id]
        defender_company = state["companies"][defender_id]
        challenger_company = state["companies"][challenger_id]
        if challenger_score.retention_priority_score >= defender_score.retention_priority_score + 7:
            allowed, gate_note = _challenger_promotion_allowed(state, challenger_id)
            if not allowed:
                if gate_note:
                    notes.append(gate_note)
                notes.append(f"{challenger_id} held in shadow_watch despite outperforming {defender_id}")
                deltas.append(
                    _make_delta(
                        challenger_company,
                        challenger_score,
                        Bucket.SHADOW_WATCH,
                        infer_thesis_status(challenger_score),
                        CurrentRoute.SHADOW_OBSERVATION,
                        state["run_state"],
                    )
                )
                continue
            notes.append(f"{challenger_id} promoted over {defender_id}")
            deltas.extend(
                [
                    _make_delta(
                        challenger_company,
                        challenger_score,
                        Bucket.SECONDARY_CANDIDATES,
                        infer_thesis_status(challenger_score),
                        CurrentRoute.ARENA_COMPETITION,
                        state["run_state"],
                    ),
                    _make_delta(
                        defender_company,
                        defender_score,
                        Bucket.ARCHIVED,
                        ThesisStatus.RETIRED if defender_score.retention_priority_score < 35 else ThesisStatus.FRAGILE,
                        CurrentRoute.ARENA_COMPETITION,
                        state["run_state"],
                    ),
                ]
            )
        else:
            notes.append(f"{challenger_id} sent to shadow_watch after tie with {defender_id}")
            deltas.append(
                _make_delta(
                    challenger_company,
                    challenger_score,
                    Bucket.SHADOW_WATCH,
                    infer_thesis_status(challenger_score),
                    CurrentRoute.SHADOW_OBSERVATION,
                    state["run_state"],
                )
            )
    return {"deltas": deltas, "merge_notes": state["merge_notes"] + notes}


def _n9_decision_engine(state: WorkflowState) -> WorkflowState:
    deltas = list(state.get("deltas", []))
    companies = state["companies"]
    scorecards = state["scorecards"]
    already_decided = {delta.entity_id for delta in deltas}

    for entity_id in state["defender_ids"]:
        if entity_id in already_decided:
            continue
        company = companies[entity_id]
        scorecard = scorecards[entity_id]
        target_bucket = company.current_bucket
        if scorecard.retention_priority_score >= 75 and company.current_bucket == Bucket.SECONDARY_CANDIDATES:
            target_bucket = Bucket.CORE_TRACKING
        elif scorecard.retention_priority_score < 45 and company.current_bucket == Bucket.CORE_TRACKING:
            target_bucket = Bucket.SECONDARY_CANDIDATES
        target_bucket, gate_note = _incumbent_upgrade_allowed(state, entity_id, company.current_bucket, target_bucket)
        if gate_note:
            state["merge_notes"].append(gate_note)
        deltas.append(
            _make_delta(
                company,
                scorecard,
                target_bucket,
                infer_thesis_status(scorecard),
                CurrentRoute.INCUMBENT_REVIEW,
                state["run_state"],
            )
        )

    for entity_id in state["challenger_ids"]:
        if entity_id in already_decided:
            continue
        company = companies[entity_id]
        scorecard = scorecards[entity_id]
        target_bucket = Bucket.SECONDARY_CANDIDATES if scorecard.retention_priority_score >= 72 else Bucket.SHADOW_WATCH
        route = CurrentRoute.CHALLENGER_SCAN
        if target_bucket != Bucket.SHADOW_WATCH:
            allowed, gate_note = _challenger_promotion_allowed(state, entity_id)
            if not allowed:
                target_bucket = Bucket.SHADOW_WATCH
                route = CurrentRoute.SHADOW_OBSERVATION
                if gate_note:
                    state["merge_notes"].append(gate_note)
        deltas.append(
            _make_delta(
                company,
                scorecard,
                target_bucket,
                infer_thesis_status(scorecard),
                route,
                state["run_state"],
            )
        )

    all_prescreen_ids = set(state.get("prescreen_results", {}).keys())
    blocked_ids = sorted(all_prescreen_ids - set(state["challenger_ids"]))
    for entity_id in blocked_ids:
        if entity_id in already_decided:
            continue
        prescreen = state["prescreen_results"][entity_id]
        entity = state["entity_map"][entity_id]
        company = companies.get(entity_id)
        if company is None:
            company = make_new_company_state(entity, Bucket.SHADOW_WATCH, prescreen.eligible_route)
            companies[entity_id] = company
        shadow_score = build_prescreen_shadow_scorecard(entity, prescreen)
        deltas.append(
            _make_delta(
                company,
                shadow_score,
                Bucket.SHADOW_WATCH,
                ThesisStatus.FRAGILE,
                prescreen.eligible_route,
                state["run_state"],
            )
        )
    return {"deltas": deltas, "merge_notes": state["merge_notes"]}


def _n10_merge(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    companies = state["companies"]
    pool: ResearchPoolState = state["pool"]
    audits: list[AuditTrailRecord] = []
    recoveries: list[ExecutionRecoveryRecord] = []
    active_members = set(pool.current_pool_members)
    shadow_members = set(pool.shadow_watch_members)
    archived_members = set(pool.archived_members)

    for delta in state["deltas"]:
        company = companies[delta.entity_id]
        try:
            updated = company.model_copy(
                update={
                    "current_bucket": delta.to_bucket,
                    "current_route": delta.route,
                    "thesis_status": delta.to_thesis_status,
                    "current_quality_score": delta.score_snapshot.current_quality_score,
                    "trajectory_score": delta.score_snapshot.trajectory_score,
                    "retention_priority_score": delta.score_snapshot.retention_priority_score,
                    "score_version": delta.score_snapshot.score_version,
                    "last_confirmed_date": datetime.now(UTC).date(),
                    "recent_thesis_summaries": [delta.rationale, *company.recent_thesis_summaries][:2],
                }
            )
            companies[delta.entity_id] = updated
            store.save_company(updated)
            active_members.discard(delta.entity_id)
            shadow_members.discard(delta.entity_id)
            archived_members.discard(delta.entity_id)
            if delta.to_bucket == Bucket.SHADOW_WATCH:
                shadow_members.add(delta.entity_id)
            elif delta.to_bucket == Bucket.ARCHIVED:
                archived_members.add(delta.entity_id)
            else:
                active_members.add(delta.entity_id)

            audit = AuditTrailRecord(
                decision_id=make_id("decision"),
                review_id=updated.review_chain_id or make_id("review"),
                input_snapshot_hash=delta.input_snapshot_hash,
                state_delta_hash=model_hash(delta),
                policy_version_set=state["run_state"].policy_version_set,
                who_computed_what={
                    "trigger": "code",
                    "scoring": "code",
                    "merge": "code",
                    "narration": "placeholder_rule_engine",
                },
                llm_output_hash="no_llm_in_mvp",
                merged_by="system",
            )
            audits.append(audit)
            store.save_audit_record(audit)
        except Exception as exc:
            recovery = ExecutionRecoveryRecord(
                recovery_id=make_id("recovery"),
                run_id=state["run_state"].run_id,
                failed_node="N10",
                failed_entities=[delta.entity_id],
                reason=str(exc),
            )
            recoveries.append(recovery)
            store.save_recovery_record(recovery)

    updated_pool = pool.model_copy(
        update={
            "current_pool_members": sorted(active_members),
            "shadow_watch_members": sorted(shadow_members),
            "archived_members": sorted(archived_members),
            "last_updated_at": datetime.now(UTC),
        }
    )
    store.save_pool(updated_pool)
    run_status = RunStatus.MERGED if not recoveries else RunStatus.PARTIAL_FAILED
    run_state = state["run_state"].model_copy(
        update={
            "run_status": run_status,
            "decision_output": {
                "deltas": [delta.model_dump(mode="json") for delta in state["deltas"]],
                "notes": state["merge_notes"],
            },
        }
    )
    store.save_run(run_state)
    return {
        "pool": updated_pool,
        "companies": companies,
        "audits": audits,
        "recoveries": recoveries,
        "run_state": run_state,
    }


def _n11_pool_reassembler(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    snapshots = store.list_pool_snapshots()
    version_number = len(snapshots) + 1
    snapshot = PoolVersionSnapshot(
        snapshot_id=make_id("snapshot"),
        run_id=state["run_state"].run_id,
        pool_id=state["pool"].pool_id,
        version_number=version_number,
        current_pool_members=state["pool"].current_pool_members,
        shadow_watch_members=state["pool"].shadow_watch_members,
        archived_members=state["pool"].archived_members,
        decision_summary=state["merge_notes"],
        policy_version_set=state["run_state"].policy_version_set,
        snapshot_hash="pending",
    )
    snapshot = snapshot.model_copy(update={"snapshot_hash": model_hash(snapshot)})
    return {"pool_snapshot": snapshot}


def _milestone_markers(company: CompanyStateRecord) -> list[str]:
    markers: list[str] = []
    if company.current_bucket == Bucket.CORE_TRACKING:
        markers.append("core_tracking")
    if company.current_bucket == Bucket.SHADOW_WATCH:
        markers.append("shadow_watch")
    if company.current_bucket == Bucket.ARCHIVED:
        markers.append("archived")
    if company.thesis_status == ThesisStatus.ACCELERATING:
        markers.append("accelerating")
    if company.thesis_status == ThesisStatus.FRAGILE:
        markers.append("fragile")
    return markers


def _n12_archive_and_version_control(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    snapshot = state["pool_snapshot"]
    store.save_pool_snapshot(snapshot)

    touched_ids = sorted({delta.entity_id for delta in state["deltas"]})
    company_folds: list[CompanyHistoryFold] = []
    companies = state["companies"]
    packets = state.get("packets", {})
    for entity_id in touched_ids:
        company = companies[entity_id]
        historical_tags = list(dict.fromkeys([*company.historical_tags, company.current_bucket.value, company.thesis_status.value]))
        summary = (
            f"{company.company_name} in {company.current_bucket.value} with thesis {company.thesis_status.value} "
            f"and retention {company.retention_priority_score:.2f}"
        )
        fold = CompanyHistoryFold(
            fold_id=make_id("fold"),
            entity_id=entity_id,
            run_id=state["run_state"].run_id,
            bucket=company.current_bucket,
            route=company.current_route,
            thesis_status=company.thesis_status,
            retention_priority_score=company.retention_priority_score,
            historical_tags=historical_tags[-6:],
            evidence_counters={
                "thesis_summaries": len(company.recent_thesis_summaries),
                "active_factor_exposures": len(company.active_factor_exposures),
                "trigger_events": len(state["run_state"].trigger_event_ids),
            },
            milestone_markers=_milestone_markers(company),
            archived_summary=summary,
        )
        company_folds.append(fold)
        store.save_company_history_fold(fold)
        store.save_company(
            company.model_copy(
                update={
                    "historical_tags": historical_tags[-6:],
                    "recent_thesis_summaries": company.recent_thesis_summaries[:2],
                }
            )
        )

    archive = ArchiveVersionRecord(
        archive_id=make_id("archive"),
        run_id=state["run_state"].run_id,
        pool_snapshot_id=snapshot.snapshot_id,
        company_fold_ids=[fold.fold_id for fold in company_folds],
        archived_entity_ids=state["pool"].archived_members,
        object_refs={
            "input_snapshot_hash": state["run_state"].input_snapshot_hash,
            "pool_snapshot_hash": snapshot.snapshot_hash,
            "packet_hash_count": str(len(packets)),
        },
        version_hash="pending",
    )
    archive = archive.model_copy(update={"version_hash": model_hash(archive)})
    store.save_archive_version_record(archive)
    return {"company_folds": company_folds, "archive_record": archive}


def _n13_dynamic_post_mortem_loop(state: WorkflowState) -> WorkflowState:
    store = _store(state)
    metrics = build_evaluation_metrics(
        run_state=state["run_state"],
        deltas=state["deltas"],
        companies=state["companies"],
        prescreens=state.get("prescreen_results", {}),
    )
    report = build_post_mortem_report(
        run_state=state["run_state"],
        metrics=metrics,
        deltas=state["deltas"],
        prescreens=state.get("prescreen_results", {}),
    )
    store.save_evaluation_metrics(metrics)
    store.save_post_mortem_report(report)
    return {"evaluation_metrics": metrics, "post_mortem_report": report}


def build_workflow():
    builder = StateGraph(WorkflowState)
    builder.add_node("n0_initialize_run", _n0_initialize_run)
    builder.add_node("trigger_triage", _trigger_triage)
    builder.add_node("web_research", _web_research)
    builder.add_node("knowledge_overlay", _knowledge_overlay)
    builder.add_node("n1_theme_decomposition", _n1_theme_decomposition)
    builder.add_node("n2_market_context_loader", _n2_market_context_loader)
    builder.add_node("n3_incumbent_health_check", _n3_incumbent_health_check)
    builder.add_node("n4_defender_selection", _n4_defender_selection)
    builder.add_node("n5_external_challenger_scan", _n5_external_challenger_scan)
    builder.add_node("n6_challenger_prescreen", _n6_challenger_prescreen)
    builder.add_node("n7_deep_scoring", _n7_deep_scoring)
    builder.add_node("n8_arena", _n8_arena)
    builder.add_node("n9_decision_engine", _n9_decision_engine)
    builder.add_node("n10_merge", _n10_merge)
    builder.add_node("n11_pool_reassembler", _n11_pool_reassembler)
    builder.add_node("n12_archive_and_version_control", _n12_archive_and_version_control)
    builder.add_node("n13_dynamic_post_mortem_loop", _n13_dynamic_post_mortem_loop)
    builder.add_edge(START, "n0_initialize_run")
    builder.add_edge("n0_initialize_run", "trigger_triage")
    builder.add_edge("trigger_triage", "web_research")
    builder.add_edge("web_research", "knowledge_overlay")
    builder.add_edge("knowledge_overlay", "n1_theme_decomposition")
    builder.add_edge("n1_theme_decomposition", "n2_market_context_loader")
    builder.add_edge("n2_market_context_loader", "n3_incumbent_health_check")
    builder.add_edge("n3_incumbent_health_check", "n4_defender_selection")
    builder.add_edge("n4_defender_selection", "n5_external_challenger_scan")
    builder.add_edge("n5_external_challenger_scan", "n6_challenger_prescreen")
    builder.add_edge("n6_challenger_prescreen", "n7_deep_scoring")
    builder.add_edge("n7_deep_scoring", "n8_arena")
    builder.add_edge("n8_arena", "n9_decision_engine")
    builder.add_edge("n9_decision_engine", "n10_merge")
    builder.add_edge("n10_merge", "n11_pool_reassembler")
    builder.add_edge("n11_pool_reassembler", "n12_archive_and_version_control")
    builder.add_edge("n12_archive_and_version_control", "n13_dynamic_post_mortem_loop")
    builder.add_edge("n13_dynamic_post_mortem_loop", END)
    return builder.compile()


def run_mvp_workflow(
    db_path: str = "data/agent.db",
    data_config_path: str | None = None,
    web_config_path: str | None = None,
    universe_id: str = "us_macro_ai",
    pool_id: str = "us_macro_ai_pool",
    run_mode: str = "periodic_review",
    macro_theme: str = "AI infrastructure resilience",
    incoming_events: list[dict[str, Any]] | None = None,
) -> WorkflowState:
    graph = build_workflow()
    return graph.invoke(
        {
            "db_path": db_path,
            "data_config_path": data_config_path,
            "web_config_path": web_config_path,
            "universe_id": universe_id,
            "pool_id": pool_id,
            "run_mode": run_mode,
            "macro_theme": macro_theme,
            "incoming_events": incoming_events or [],
        }
    )


def apply_manual_override(
    db_path: str,
    entity_id: str,
    override_field: str,
    new_value: str,
    reason: str,
    operator: str = "human_operator",
    effective_hours: int = 24,
    pool_id: str | None = None,
) -> ManualOverrideRecord:
    store = SQLiteStateStore(db_path)
    company = store.load_company(entity_id)
    pool = _resolve_pool_for_entity(store, entity_id, pool_id)
    if company is None or pool is None:
        raise ValueError(f"Unknown entity_id: {entity_id}")

    allowed_fields = {"current_bucket", "thesis_status"}
    if override_field not in allowed_fields:
        raise ValueError(f"Unsupported override field: {override_field}")

    updates: dict[str, Any] = {}
    current_raw_value = getattr(company, override_field)
    old_value = current_raw_value.value if hasattr(current_raw_value, "value") else str(current_raw_value)
    if override_field == "current_bucket":
        target_bucket = Bucket(new_value)
        validate_bucket_transition(company.current_bucket, target_bucket)
        updates["current_bucket"] = target_bucket
        active_members = set(pool.current_pool_members)
        shadow_members = set(pool.shadow_watch_members)
        archived_members = set(pool.archived_members)
        active_members.discard(entity_id)
        shadow_members.discard(entity_id)
        archived_members.discard(entity_id)
        if target_bucket == Bucket.SHADOW_WATCH:
            shadow_members.add(entity_id)
        elif target_bucket == Bucket.ARCHIVED:
            archived_members.add(entity_id)
        else:
            active_members.add(entity_id)
        store.save_pool(
            pool.model_copy(
                update={
                    "current_pool_members": sorted(active_members),
                    "shadow_watch_members": sorted(shadow_members),
                    "archived_members": sorted(archived_members),
                    "last_updated_at": datetime.now(UTC),
                }
            )
        )
    elif override_field == "thesis_status":
        target_status = ThesisStatus(new_value)
        validate_thesis_transition(company.thesis_status, target_status)
        updates["thesis_status"] = target_status

    updates["manual_override_flags"] = [*company.manual_override_flags, override_field]
    updates["current_route"] = CurrentRoute.MANUAL_REVIEW
    updated_company = company.model_copy(update=updates)
    store.save_company(updated_company)

    override = ManualOverrideRecord(
        override_id=make_id("override"),
        target_object_id=entity_id,
        override_field=override_field,
        old_value=old_value,
        new_value=new_value,
        reason=reason,
        operator=operator,
        effective_until=datetime.now(UTC).replace(microsecond=0) + timedelta(hours=effective_hours),
    )
    store.save_manual_override(override)
    store.save_audit_record(
        AuditTrailRecord(
            decision_id=make_id("decision"),
            review_id=updated_company.review_chain_id or make_id("review"),
            input_snapshot_hash=model_hash(
                {
                    "entity_id": entity_id,
                    "override_field": override_field,
                    "new_value": new_value,
                    "reason": reason,
                }
            ),
            state_delta_hash=model_hash(override),
            policy_version_set=pool.active_policy_versions,
            who_computed_what={
                "override": operator,
                "validation": "code",
                "merge": "code",
            },
            llm_output_hash="manual_override_no_llm",
            merged_by=operator,
        )
    )
    return override
