from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from .models import (
    ArchiveVersionRecord,
    AuditTrailRecord,
    CompanyHistoryFold,
    CompanyStateRecord,
    DataSourceHealthRecord,
    EvidenceFusionSummary,
    EvaluationMetricsRecord,
    ExecutionRecoveryRecord,
    FactorRegistryItem,
    KnowledgeCollectionRecord,
    KnowledgeContextRecord,
    KnowledgeDocument,
    KnowledgeSlice,
    KnowledgeWeightPolicy,
    MarketContextSnapshot,
    ManualOverrideRecord,
    PoolVersionSnapshot,
    PostMortemReport,
    PrescreenDecision,
    ResearchPoolState,
    RunState,
    WebResearchRecord,
    ThemeDecomposition,
    TriggerEventRecord,
    UniverseState,
)

T = TypeVar("T", bound=BaseModel)


class SQLiteStateStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS universe_state (
                    universe_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_pool_state (
                    pool_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS company_state_record (
                    entity_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_state (
                    run_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trigger_event (
                    event_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_trail (
                    decision_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS manual_override (
                    override_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS execution_recovery (
                    recovery_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS theme_decomposition (
                    decomposition_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS market_context_snapshot (
                    context_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS data_source_health (
                    record_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS factor_registry (
                    factor_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS prescreen_decision (
                    prescreen_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pool_version_snapshot (
                    snapshot_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS company_history_fold (
                    fold_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS archive_version_record (
                    archive_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evaluation_metrics_record (
                    metrics_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS post_mortem_report (
                    report_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS web_research_record (
                    record_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evidence_fusion_summary (
                    summary_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_document (
                    document_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_slice (
                    slice_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_collection (
                    collection_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_weight_policy (
                    policy_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_context_record (
                    context_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                """
            )

    def _upsert_model(self, table: str, key_name: str, key_value: str, model: BaseModel) -> None:
        payload = json.dumps(model.model_dump(mode="json"), ensure_ascii=True, sort_keys=True)
        with closing(self._connect()) as conn:
            conn.execute(
                f"INSERT INTO {table} ({key_name}, payload) VALUES (?, ?) "
                f"ON CONFLICT({key_name}) DO UPDATE SET payload = excluded.payload",
                (key_value, payload),
            )
            conn.commit()

    def _load_model(self, table: str, key_name: str, key_value: str, model_cls: type[T]) -> T | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                f"SELECT payload FROM {table} WHERE {key_name} = ?",
                (key_value,),
            ).fetchone()
        if not row:
            return None
        return model_cls.model_validate(json.loads(row["payload"]))

    def save_universe(self, universe: UniverseState) -> None:
        self._upsert_model("universe_state", "universe_id", universe.universe_id, universe)

    def load_universe(self, universe_id: str) -> UniverseState | None:
        return self._load_model("universe_state", "universe_id", universe_id, UniverseState)

    def list_universes(self) -> list[UniverseState]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM universe_state ORDER BY universe_id").fetchall()
        return [UniverseState.model_validate(json.loads(row["payload"])) for row in rows]

    def save_pool(self, pool: ResearchPoolState) -> None:
        self._upsert_model("research_pool_state", "pool_id", pool.pool_id, pool)

    def load_pool(self, pool_id: str) -> ResearchPoolState | None:
        return self._load_model("research_pool_state", "pool_id", pool_id, ResearchPoolState)

    def list_pools(self) -> list[ResearchPoolState]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM research_pool_state ORDER BY pool_id").fetchall()
        return [ResearchPoolState.model_validate(json.loads(row["payload"])) for row in rows]

    def save_company(self, company: CompanyStateRecord) -> None:
        self._upsert_model("company_state_record", "entity_id", company.entity_id, company)

    def load_company(self, entity_id: str) -> CompanyStateRecord | None:
        return self._load_model("company_state_record", "entity_id", entity_id, CompanyStateRecord)

    def list_companies(self) -> list[CompanyStateRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM company_state_record ORDER BY entity_id").fetchall()
        return [CompanyStateRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_run(self, run_state: RunState) -> None:
        self._upsert_model("run_state", "run_id", run_state.run_id, run_state)

    def load_run(self, run_id: str) -> RunState | None:
        return self._load_model("run_state", "run_id", run_id, RunState)

    def list_runs(self) -> list[RunState]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM run_state ORDER BY run_id").fetchall()
        return [RunState.model_validate(json.loads(row["payload"])) for row in rows]

    def save_trigger_event(self, event: TriggerEventRecord) -> None:
        self._upsert_model("trigger_event", "event_id", event.event_id, event)

    def list_trigger_events(self) -> list[TriggerEventRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM trigger_event ORDER BY event_id").fetchall()
        return [TriggerEventRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_audit_record(self, record: AuditTrailRecord) -> None:
        self._upsert_model("audit_trail", "decision_id", record.decision_id, record)

    def list_audit_records(self) -> list[AuditTrailRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM audit_trail ORDER BY decision_id").fetchall()
        return [AuditTrailRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_manual_override(self, record: ManualOverrideRecord) -> None:
        self._upsert_model("manual_override", "override_id", record.override_id, record)

    def list_manual_overrides(self) -> list[ManualOverrideRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM manual_override ORDER BY override_id").fetchall()
        return [ManualOverrideRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_recovery_record(self, record: ExecutionRecoveryRecord) -> None:
        self._upsert_model("execution_recovery", "recovery_id", record.recovery_id, record)

    def list_recovery_records(self) -> list[ExecutionRecoveryRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM execution_recovery ORDER BY recovery_id").fetchall()
        return [ExecutionRecoveryRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_theme_decomposition(self, decomposition: ThemeDecomposition) -> None:
        self._upsert_model("theme_decomposition", "decomposition_id", decomposition.decomposition_id, decomposition)

    def list_theme_decompositions(self) -> list[ThemeDecomposition]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM theme_decomposition ORDER BY decomposition_id").fetchall()
        return [ThemeDecomposition.model_validate(json.loads(row["payload"])) for row in rows]

    def save_market_context(self, snapshot: MarketContextSnapshot) -> None:
        self._upsert_model("market_context_snapshot", "context_id", snapshot.context_id, snapshot)

    def list_market_contexts(self) -> list[MarketContextSnapshot]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM market_context_snapshot ORDER BY context_id").fetchall()
        return [MarketContextSnapshot.model_validate(json.loads(row["payload"])) for row in rows]

    def save_source_health(self, record: DataSourceHealthRecord) -> None:
        self._upsert_model("data_source_health", "record_id", record.record_id, record)

    def list_source_health(self) -> list[DataSourceHealthRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM data_source_health ORDER BY record_id").fetchall()
        return [DataSourceHealthRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_factor_registry_item(self, item: FactorRegistryItem) -> None:
        self._upsert_model("factor_registry", "factor_id", item.factor_id, item)

    def list_factor_registry_items(self) -> list[FactorRegistryItem]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM factor_registry ORDER BY factor_id").fetchall()
        return [FactorRegistryItem.model_validate(json.loads(row["payload"])) for row in rows]

    def save_prescreen_decision(self, decision: PrescreenDecision) -> None:
        self._upsert_model("prescreen_decision", "prescreen_id", decision.prescreen_id, decision)

    def list_prescreen_decisions(self) -> list[PrescreenDecision]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM prescreen_decision ORDER BY prescreen_id").fetchall()
        return [PrescreenDecision.model_validate(json.loads(row["payload"])) for row in rows]

    def save_pool_snapshot(self, snapshot: PoolVersionSnapshot) -> None:
        self._upsert_model("pool_version_snapshot", "snapshot_id", snapshot.snapshot_id, snapshot)

    def list_pool_snapshots(self) -> list[PoolVersionSnapshot]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM pool_version_snapshot ORDER BY snapshot_id").fetchall()
        return [PoolVersionSnapshot.model_validate(json.loads(row["payload"])) for row in rows]

    def save_company_history_fold(self, fold: CompanyHistoryFold) -> None:
        self._upsert_model("company_history_fold", "fold_id", fold.fold_id, fold)

    def list_company_history_folds(self) -> list[CompanyHistoryFold]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM company_history_fold ORDER BY fold_id").fetchall()
        return [CompanyHistoryFold.model_validate(json.loads(row["payload"])) for row in rows]

    def save_archive_version_record(self, record: ArchiveVersionRecord) -> None:
        self._upsert_model("archive_version_record", "archive_id", record.archive_id, record)

    def list_archive_version_records(self) -> list[ArchiveVersionRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM archive_version_record ORDER BY archive_id").fetchall()
        return [ArchiveVersionRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_evaluation_metrics(self, record: EvaluationMetricsRecord) -> None:
        self._upsert_model("evaluation_metrics_record", "metrics_id", record.metrics_id, record)

    def list_evaluation_metrics(self) -> list[EvaluationMetricsRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM evaluation_metrics_record ORDER BY metrics_id").fetchall()
        return [EvaluationMetricsRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_post_mortem_report(self, report: PostMortemReport) -> None:
        self._upsert_model("post_mortem_report", "report_id", report.report_id, report)

    def list_post_mortem_reports(self) -> list[PostMortemReport]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM post_mortem_report ORDER BY report_id").fetchall()
        return [PostMortemReport.model_validate(json.loads(row["payload"])) for row in rows]

    def save_web_research_record(self, record: WebResearchRecord) -> None:
        self._upsert_model("web_research_record", "record_id", record.record_id, record)

    def list_web_research_records(self) -> list[WebResearchRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM web_research_record ORDER BY record_id").fetchall()
        return [WebResearchRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_evidence_fusion_summary(self, summary: EvidenceFusionSummary) -> None:
        self._upsert_model("evidence_fusion_summary", "summary_id", summary.summary_id, summary)

    def list_evidence_fusion_summaries(self) -> list[EvidenceFusionSummary]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM evidence_fusion_summary ORDER BY summary_id").fetchall()
        return [EvidenceFusionSummary.model_validate(json.loads(row["payload"])) for row in rows]

    def save_knowledge_document(self, document: KnowledgeDocument) -> None:
        self._upsert_model("knowledge_document", "document_id", document.document_id, document)

    def list_knowledge_documents(self) -> list[KnowledgeDocument]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM knowledge_document ORDER BY document_id").fetchall()
        return [KnowledgeDocument.model_validate(json.loads(row["payload"])) for row in rows]

    def save_knowledge_slice(self, item: KnowledgeSlice) -> None:
        self._upsert_model("knowledge_slice", "slice_id", item.slice_id, item)

    def list_knowledge_slices(self) -> list[KnowledgeSlice]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM knowledge_slice ORDER BY slice_id").fetchall()
        return [KnowledgeSlice.model_validate(json.loads(row["payload"])) for row in rows]

    def save_knowledge_collection(self, item: KnowledgeCollectionRecord) -> None:
        self._upsert_model("knowledge_collection", "collection_id", item.collection_id, item)

    def list_knowledge_collections(self) -> list[KnowledgeCollectionRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM knowledge_collection ORDER BY collection_id").fetchall()
        return [KnowledgeCollectionRecord.model_validate(json.loads(row["payload"])) for row in rows]

    def save_knowledge_policy(self, item: KnowledgeWeightPolicy) -> None:
        self._upsert_model("knowledge_weight_policy", "policy_id", item.policy_id, item)

    def list_knowledge_policies(self) -> list[KnowledgeWeightPolicy]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM knowledge_weight_policy ORDER BY policy_id").fetchall()
        return [KnowledgeWeightPolicy.model_validate(json.loads(row["payload"])) for row in rows]

    def save_knowledge_context(self, item: KnowledgeContextRecord) -> None:
        self._upsert_model("knowledge_context_record", "context_id", item.context_id, item)

    def list_knowledge_contexts(self) -> list[KnowledgeContextRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT payload FROM knowledge_context_record ORDER BY context_id").fetchall()
        return [KnowledgeContextRecord.model_validate(json.loads(row["payload"])) for row in rows]
