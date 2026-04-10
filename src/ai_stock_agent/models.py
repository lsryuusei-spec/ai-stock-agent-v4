from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


class RunMode(str, Enum):
    INITIAL_BUILD = "initial_build"
    PERIODIC_REVIEW = "periodic_review"
    EVENT_DRIVEN_REFRESH = "event_driven_refresh"
    RECOVERY_REPLAY = "recovery_replay"


class RunStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PARTIAL_FAILED = "partial_failed"
    MERGED = "merged"
    ABORTED = "aborted"
    REPLAYED = "replayed"


class Bucket(str, Enum):
    CORE_TRACKING = "core_tracking"
    SECONDARY_CANDIDATES = "secondary_candidates"
    HIGH_BETA_WATCH = "high_beta_watch"
    SHADOW_WATCH = "shadow_watch"
    ARCHIVED = "archived"


class ThesisStatus(str, Enum):
    FORMING = "forming"
    VALIDATED = "validated"
    ACCELERATING = "accelerating"
    FRAGILE = "fragile"
    BROKEN = "broken"
    RETIRED = "retired"


class CurrentRoute(str, Enum):
    INCUMBENT_REVIEW = "incumbent_review"
    CHALLENGER_SCAN = "challenger_scan"
    ARENA_COMPETITION = "arena_competition"
    SHADOW_OBSERVATION = "shadow_observation"
    ARCHIVE_MONITORING = "archive_monitoring"
    MANUAL_REVIEW = "manual_review"


class EventType(str, Enum):
    EARNINGS = "earnings"
    MACRO = "macro"
    GUIDANCE = "guidance"
    PRICE_BREAK = "price_break"
    NEWS = "news"
    MANUAL = "manual"


class SourceType(str, Enum):
    PIPELINE = "pipeline"
    NEWS = "news"
    FILING = "filing"
    MANUAL = "manual"
    SYSTEM = "system"


class WakeScope(str, Enum):
    ENTITY = "entity"
    BUCKET = "bucket"
    POOL = "pool"
    UNIVERSE = "universe"


class TransitionError(ValueError):
    pass


class EntityProfile(BaseModel):
    entity_id: str
    ticker: str
    company_name: str
    sector: str
    market_cap: float
    website: str | None = None
    business_summary: str | None = None
    english_name: str | None = None
    liquidity_score: float = Field(ge=0, le=100)
    info_score: float = Field(ge=0, le=100)
    tradability_score: float = Field(ge=0, le=100)
    base_quality: float = Field(ge=0, le=100)
    industry_position_score: float = Field(ge=0, le=100)
    macro_alignment_score: float = Field(ge=0, le=100)
    valuation_score: float = Field(ge=0, le=100)
    catalyst_score: float = Field(ge=0, le=100)
    risk_penalty: float = Field(ge=0, le=100)
    evidence_freshness_days: int = Field(ge=0)
    active_factor_exposures: list[str] = Field(default_factory=list)


class UniverseState(BaseModel):
    universe_id: str
    market: str
    effective_date: date
    eligible_entities: list[EntityProfile]
    excluded_entities: list[str] = Field(default_factory=list)
    entity_mapping_version: str
    universe_rules_version: str


class PoolCapacityPolicy(BaseModel):
    core_tracking_min: int = 6
    core_tracking_max: int = 8
    secondary_candidates_min: int = 8
    secondary_candidates_max: int = 10
    high_beta_watch_min: int = 4
    high_beta_watch_max: int = 6
    shadow_watch_max_cycles: int = 3


class PolicyVersionSet(BaseModel):
    schema_version: str = "2026.03"
    score_version: str = "score_v1"
    factor_registry_version: str = "factor_registry_v1"
    policy_version: str = "policy_v1"
    prompt_version: str = "prompt_v1"
    merge_engine_version: str = "merge_v1"


class WeightCalibrationPolicy(BaseModel):
    fundamental_cap: float = 40
    industry_cap: float = 25
    macro_cap: float = 15
    valuation_cap: float = 10
    catalyst_cap: float = 10
    quant_verified_multiplier: float = 1.2
    filing_based_multiplier: float = 1.1
    management_guidance_multiplier: float = 0.95
    news_sentiment_multiplier: float = 0.8
    stale_penalty_per_day: float = 0.35
    max_breakthrough_bonus: float = 15
    quality_floor_for_bonus: float = 55
    prescreen_min_info_score: float = 70
    prescreen_min_liquidity_score: float = 70
    prescreen_max_risk_penalty: float = 60
    prescreen_min_factor_overlap: int = 1
    prescreen_data_block_penalty: float = 8


class ResearchPoolState(BaseModel):
    pool_id: str
    market: str
    last_updated_at: datetime = Field(default_factory=utc_now)
    current_pool_members: list[str] = Field(default_factory=list)
    shadow_watch_members: list[str] = Field(default_factory=list)
    archived_members: list[str] = Field(default_factory=list)
    pool_capacity_policy: PoolCapacityPolicy = Field(default_factory=PoolCapacityPolicy)
    active_policy_versions: PolicyVersionSet = Field(default_factory=PolicyVersionSet)


class CompanyStateRecord(BaseModel):
    entity_id: str
    ticker: str
    company_name: str
    current_bucket: Bucket
    current_route: CurrentRoute
    thesis_status: ThesisStatus
    current_quality_score: float = 0.0
    trajectory_score: float = 0.0
    retention_priority_score: float = 0.0
    recent_thesis_summaries: list[str] = Field(default_factory=list)
    historical_tags: list[str] = Field(default_factory=list)
    last_primary_evidence_date: date
    last_confirmed_date: date
    freshness_window_days: int = 30
    staleness_penalty: float = 0.0
    staleness_level: str = "fresh"
    manual_override_flags: list[str] = Field(default_factory=list)
    active_factor_exposures: list[str] = Field(default_factory=list)
    score_version: str = "score_v1"
    review_chain_id: str = ""
    latest_packet_hash: str | None = None
    macro_context_tag: str | None = None


class DataSourceManifestItem(BaseModel):
    source_name: str
    source_type: SourceType
    as_of: datetime
    field_coverage: float = Field(ge=0, le=1)


class FrozenQuantPacket(BaseModel):
    as_of_date: date
    data_freshness_hours: int
    raw_metrics: dict[str, Any]
    derived_metrics: dict[str, Any]
    peer_percentiles: dict[str, Any]
    valuation_snapshot: dict[str, Any]
    tradability_snapshot: dict[str, Any]
    source_manifest: list[DataSourceManifestItem]
    packet_hash: str


class ThemeSlice(BaseModel):
    slice_id: str
    label: str
    weight: float = Field(ge=0, le=1)
    rationale: str
    linked_factors: list[str] = Field(default_factory=list)


class ScenarioNode(BaseModel):
    scenario_id: str
    name: str
    probability: float = Field(ge=0, le=1)
    key_drivers: list[str] = Field(default_factory=list)
    implications: list[str] = Field(default_factory=list)


class ThemeDecomposition(BaseModel):
    decomposition_id: str
    macro_theme: str
    as_of: datetime = Field(default_factory=utc_now)
    theme_slices: list[ThemeSlice]
    scenario_tree: list[ScenarioNode]
    dominant_regime: str
    summary: str
    decomposition_hash: str


class MarketIndicator(BaseModel):
    indicator_id: str
    label: str
    value: float
    unit: str
    direction: str
    as_of: datetime
    source_name: str


class DataSourceHealthRecord(BaseModel):
    record_id: str
    data_domain: str
    primary_source: str
    backup_source: str | None = None
    field_completeness: float = Field(ge=0, le=1)
    latency_hours: float = Field(ge=0)
    status: str
    degradation_path: str
    checked_at: datetime = Field(default_factory=utc_now)


class DataProviderConfig(BaseModel):
    provider_id: str
    data_domain: str
    mode: str
    priority: int
    provider_kind: str = "mock"
    enabled: bool = True
    latency_hours: float = 0.0
    field_completeness: float = 1.0
    base_url: str | None = None
    endpoint: str | None = None
    file_path: str | None = None
    auth_env_var: str | None = None
    timeout_seconds: float = 10.0
    static_values: dict[str, Any] = Field(default_factory=dict)
    provider_options: dict[str, Any] = Field(default_factory=dict)


class AdapterPayload(BaseModel):
    data_domain: str
    values: dict[str, Any]
    source_manifest: list[DataSourceManifestItem] = Field(default_factory=list)
    health_record: DataSourceHealthRecord
    degraded: bool = False
    missing_reason: str | None = None


class WebResearchProviderConfig(BaseModel):
    provider_id: str
    provider_kind: str = "mock"
    enabled: bool = True
    base_url: str | None = None
    endpoint: str | None = None
    file_path: str | None = None
    auth_env_var: str | None = None
    timeout_seconds: float = 10.0
    max_results: int = 5
    domains: list[str] = Field(default_factory=list)
    static_keywords: list[str] = Field(default_factory=list)
    provider_options: dict[str, Any] = Field(default_factory=dict)


class WebEvidenceItem(BaseModel):
    title: str
    snippet: str
    url: str
    source_name: str
    published_at: datetime
    entity_id: str | None = None
    relevance_score: float = Field(ge=0, le=1)
    source_tier: str = "unrated"
    source_score: float = Field(default=0.6, ge=0, le=1)
    content_hash: str | None = None
    evidence_type: str


class SourceCollectionDiagnostic(BaseModel):
    target: str
    status: str
    item_count: int = Field(default=0, ge=0)
    detail: str = ""
    request_ref: str | None = None
    latency_ms: float = Field(default=0.0, ge=0)


class WebResearchRecord(BaseModel):
    record_id: str
    run_id: str
    provider_id: str
    macro_theme: str
    query: str
    entity_id: str | None = None
    evidence_items: list[WebEvidenceItem] = Field(default_factory=list)
    collection_diagnostics: list[SourceCollectionDiagnostic] = Field(default_factory=list)
    summary: str
    status: str
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeDocument(BaseModel):
    document_id: str
    title: str
    topic_key: str | None = None
    version: int = 1
    layer_hint: str | None = None
    source_name: str
    source_type: str
    region: str = "cn"
    document_type: str = "article"
    published_at: datetime | None = None
    raw_content: str
    cleaned_content: str
    summary: str
    tags: list[str] = Field(default_factory=list)
    status: str = "active"
    supersedes_document_id: str | None = None
    superseded_by_document_id: str | None = None
    content_hash: str
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeSlice(BaseModel):
    slice_id: str
    document_id: str
    topic_key: str | None = None
    version: int = 1
    layer: str
    subtype: str
    title: str
    slice_text: str
    claim: str
    stance: str = "neutral"
    confidence: float = Field(default=0.5, ge=0, le=1)
    region: str = "cn"
    market_scope: str = "macro"
    topic_tags: list[str] = Field(default_factory=list)
    entity_tags: list[str] = Field(default_factory=list)
    crowding_score: float = Field(default=0.0, ge=0, le=1)
    principle_type: str | None = None
    action_binding: str = "advisory"
    collection_key: str
    status: str = "active"
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    source_type: str = "article"
    source_name: str = ""
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeCollectionRecord(BaseModel):
    collection_id: str
    collection_key: str
    topic_key: str | None = None
    layer: str
    label: str
    region: str = "cn"
    slice_ids: list[str] = Field(default_factory=list)
    status: str = "active"
    updated_at: datetime = Field(default_factory=utc_now)


class KnowledgeWeightPolicy(BaseModel):
    policy_id: str
    macro_weight: float = Field(ge=0, le=1)
    consensus_weight: float = Field(ge=0, le=1)
    principle_weight: float = Field(ge=0, le=1)
    consensus_positive_cap: float = Field(default=0.0, ge=0, le=1)
    consensus_negative_floor: float = Field(default=-0.12, le=0, ge=-1)
    principle_mode: str = "light_gate"
    consensus_mode: str = "crowding_risk_only"
    notes: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeLayerInsight(BaseModel):
    layer: str
    score: float = Field(ge=-1, le=1)
    confidence: float = Field(ge=0, le=1)
    summary: str
    slice_ids: list[str] = Field(default_factory=list)
    matched_topics: list[str] = Field(default_factory=list)
    action_mode: str = "advisory"
    risk_flags: list[str] = Field(default_factory=list)


class KnowledgeContextRecord(BaseModel):
    context_id: str
    run_id: str
    macro_theme: str
    region: str = "cn"
    policy_id: str
    macro_signal: KnowledgeLayerInsight
    consensus_signal: KnowledgeLayerInsight
    principle_signal: KnowledgeLayerInsight
    overall_summary: str
    topic_version_signals: list[dict[str, Any]] = Field(default_factory=list)
    topic_version_summary: str | None = None
    created_at: datetime = Field(default_factory=utc_now)


class EvidenceChannelConfidence(BaseModel):
    channel: str
    confidence_score: float = Field(ge=0, le=1)
    freshness_hours: float = Field(ge=0)
    source_count: int = Field(ge=0)
    source_names: list[str] = Field(default_factory=list)
    status: str = "ok"


class EvidenceFusionSummary(BaseModel):
    summary_id: str
    run_id: str
    entity_id: str | None = None
    scope: str
    overall_confidence: float = Field(ge=0, le=1)
    hard_data_confidence: float = Field(ge=0, le=1)
    web_confidence: float = Field(ge=0, le=1)
    official_evidence_count: int = Field(ge=0)
    source_diversity: int = Field(ge=0)
    confidence_label: str
    channels: list[EvidenceChannelConfidence] = Field(default_factory=list)
    summary: str
    created_at: datetime = Field(default_factory=utc_now)


class MarketContextSnapshot(BaseModel):
    context_id: str
    macro_theme: str
    as_of: datetime = Field(default_factory=utc_now)
    regime: str
    regime_confidence: float = Field(ge=0, le=1)
    indicators: list[MarketIndicator]
    scenario_bias: dict[str, float]
    source_health: list[DataSourceHealthRecord]
    context_summary: str
    context_status: str
    crowding_risk_score: float = Field(default=0.0, ge=0, le=1)
    principle_constraint_score: float = Field(default=0.0, ge=0, le=1)
    knowledge_overlay_summary: str | None = None
    context_hash: str


class FactorRegistryItem(BaseModel):
    factor_id: str
    factor_name: str
    factor_type: str
    scope: str
    evidence_requirements: list[str]
    budget_cap: float = Field(ge=0, le=100)
    multiplier_rule: str
    cluster_id: str
    exclusion_rules: list[str] = Field(default_factory=list)
    shadow_mode: bool = False
    shadow_cycles_required: int = 0
    promotion_criteria: str
    retirement_criteria: str


class PrescreenDecision(BaseModel):
    prescreen_id: str
    entity_id: str
    passed: bool
    decision: str
    eligible_route: CurrentRoute
    factor_overlap_count: int = 0
    passed_factors: list[str] = Field(default_factory=list)
    blocked_reasons: list[str] = Field(default_factory=list)
    confidence_multiplier: float = Field(ge=0)
    budget_usage: dict[str, float] = Field(default_factory=dict)
    context_status: str
    created_at: datetime = Field(default_factory=utc_now)


class TriggerEventRecord(BaseModel):
    event_id: str
    event_type: EventType
    event_time: datetime
    source_type: SourceType
    source_ref: str
    event_deduplication_key: str
    wake_scope: WakeScope
    impacted_entities: list[str]
    trigger_confidence: float = Field(ge=0, le=1)
    source_quality_score: float = Field(default=0.0, ge=0, le=1)
    cooldown_group: str
    parent_event_id: str | None = None


class ScoreCard(BaseModel):
    entity_id: str
    current_quality_score: float
    trajectory_score: float
    retention_priority_score: float
    valuation_component: float
    freshness_component: float
    breakthrough_bonus_score: float
    risk_penalty_component: float
    score_version: str
    notes: list[str] = Field(default_factory=list)


class StateDelta(BaseModel):
    delta_id: str
    entity_id: str
    from_bucket: Bucket | None = None
    to_bucket: Bucket | None = None
    from_thesis_status: ThesisStatus | None = None
    to_thesis_status: ThesisStatus | None = None
    route: CurrentRoute
    rationale: str
    score_snapshot: ScoreCard
    trigger_event_ids: list[str] = Field(default_factory=list)
    input_snapshot_hash: str
    idempotency_key: str
    created_at: datetime = Field(default_factory=utc_now)


class AuditTrailRecord(BaseModel):
    decision_id: str
    review_id: str
    input_snapshot_hash: str
    state_delta_hash: str
    policy_version_set: PolicyVersionSet
    who_computed_what: dict[str, str]
    llm_output_hash: str
    merged_by: str
    merged_at: datetime = Field(default_factory=utc_now)


class ManualOverrideRecord(BaseModel):
    override_id: str
    target_object_id: str
    override_field: str
    old_value: str
    new_value: str
    reason: str
    operator: str
    effective_until: datetime
    created_at: datetime = Field(default_factory=utc_now)


class ExecutionRecoveryRecord(BaseModel):
    recovery_id: str
    run_id: str
    failed_node: str
    failed_entities: list[str]
    reason: str
    created_at: datetime = Field(default_factory=utc_now)
    recovered_at: datetime | None = None


class PoolVersionSnapshot(BaseModel):
    snapshot_id: str
    run_id: str
    pool_id: str
    version_number: int
    current_pool_members: list[str]
    shadow_watch_members: list[str]
    archived_members: list[str]
    decision_summary: list[str] = Field(default_factory=list)
    policy_version_set: PolicyVersionSet
    created_at: datetime = Field(default_factory=utc_now)
    snapshot_hash: str


class CompanyHistoryFold(BaseModel):
    fold_id: str
    entity_id: str
    run_id: str
    bucket: Bucket
    route: CurrentRoute
    thesis_status: ThesisStatus
    retention_priority_score: float
    historical_tags: list[str] = Field(default_factory=list)
    evidence_counters: dict[str, int] = Field(default_factory=dict)
    milestone_markers: list[str] = Field(default_factory=list)
    archived_summary: str
    created_at: datetime = Field(default_factory=utc_now)


class ArchiveVersionRecord(BaseModel):
    archive_id: str
    run_id: str
    pool_snapshot_id: str
    company_fold_ids: list[str] = Field(default_factory=list)
    archived_entity_ids: list[str] = Field(default_factory=list)
    object_refs: dict[str, str] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    version_hash: str


class EvaluationMetricsRecord(BaseModel):
    metrics_id: str
    run_id: str
    entry_hit_rate: float = 0.0
    challenger_promotion_success_rate: float = 0.0
    defender_retention_quality: float = 0.0
    false_positive_trigger_rate: float = 0.0
    false_negative_miss_rate: float = 0.0
    score_drift_by_version: dict[str, float] = Field(default_factory=dict)
    shadow_to_production_factor_win_rate: float = 0.0
    created_at: datetime = Field(default_factory=utc_now)


class PostMortemReport(BaseModel):
    report_id: str
    run_id: str
    metrics_id: str
    wrong_judgment_cases: list[str] = Field(default_factory=list)
    delayed_judgment_cases: list[str] = Field(default_factory=list)
    factor_bloat_cases: list[str] = Field(default_factory=list)
    false_trigger_cases: list[str] = Field(default_factory=list)
    state_transition_anomalies: list[str] = Field(default_factory=list)
    summary: str
    created_at: datetime = Field(default_factory=utc_now)


class RunState(BaseModel):
    run_id: str
    created_at: datetime | None = None
    run_mode: RunMode
    run_status: RunStatus = RunStatus.CREATED
    market: str
    macro_theme: str
    trigger_event_ids: list[str] = Field(default_factory=list)
    wake_scope: WakeScope = WakeScope.POOL
    input_snapshot_hash: str
    policy_version_set: PolicyVersionSet = Field(default_factory=PolicyVersionSet)
    incumbent_review_set: list[str] = Field(default_factory=list)
    challenger_set: list[str] = Field(default_factory=list)
    process_trace: list[dict[str, Any]] = Field(default_factory=list)
    decision_output: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    parent_run_id: str | None = None
