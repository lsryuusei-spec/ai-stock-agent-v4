from __future__ import annotations

from collections import defaultdict

from .models import (
    CompanyStateRecord,
    EvaluationMetricsRecord,
    PostMortemReport,
    PrescreenDecision,
    RunState,
    StateDelta,
)
from .utils import make_id


def build_evaluation_metrics(
    run_state: RunState,
    deltas: list[StateDelta],
    companies: dict[str, CompanyStateRecord],
    prescreens: dict[str, PrescreenDecision],
) -> EvaluationMetricsRecord:
    challenger_attempts = max(1, len(prescreens))
    challenger_promotions = sum(
        1 for delta in deltas if delta.from_bucket is not None and delta.from_bucket.value == "shadow_watch" and delta.to_bucket.value == "secondary_candidates"
    )
    defender_kept = [
        companies[delta.entity_id].retention_priority_score
        for delta in deltas
        if delta.route.value == "incumbent_review" and delta.to_bucket == delta.from_bucket
    ]
    false_positive_count = max(0, len(run_state.trigger_event_ids) - len({delta.entity_id for delta in deltas}))
    shadow_to_production = sum(
        1 for delta in deltas if delta.from_bucket is not None and delta.from_bucket.value == "shadow_watch" and delta.to_bucket.value != "shadow_watch"
    )

    score_drift: dict[str, list[float]] = defaultdict(list)
    for delta in deltas:
        score_drift[delta.score_snapshot.score_version].append(delta.score_snapshot.retention_priority_score)

    return EvaluationMetricsRecord(
        metrics_id=make_id("metrics"),
        run_id=run_state.run_id,
        entry_hit_rate=round(challenger_promotions / challenger_attempts, 4),
        challenger_promotion_success_rate=round(challenger_promotions / challenger_attempts, 4),
        defender_retention_quality=round(sum(defender_kept) / len(defender_kept), 4) if defender_kept else 0.0,
        false_positive_trigger_rate=round(false_positive_count / max(1, len(run_state.trigger_event_ids)), 4),
        false_negative_miss_rate=0.0,
        score_drift_by_version={
            version: round(sum(values) / len(values), 4)
            for version, values in score_drift.items()
        },
        shadow_to_production_factor_win_rate=round(shadow_to_production / challenger_attempts, 4),
    )


def build_post_mortem_report(
    run_state: RunState,
    metrics: EvaluationMetricsRecord,
    deltas: list[StateDelta],
    prescreens: dict[str, PrescreenDecision],
) -> PostMortemReport:
    wrong_judgments = [
        f"{delta.entity_id}: archived while retention remained {delta.score_snapshot.retention_priority_score:.2f}"
        for delta in deltas
        if delta.to_bucket.value == "archived" and delta.score_snapshot.retention_priority_score >= 40
    ]
    delayed_judgments = [
        f"{delta.entity_id}: promotion only happened after shadow watch despite retention {delta.score_snapshot.retention_priority_score:.2f}"
        for delta in deltas
        if delta.from_bucket is not None
        and delta.from_bucket.value == "shadow_watch"
        and delta.to_bucket.value == "secondary_candidates"
        and delta.score_snapshot.retention_priority_score >= 70
    ]
    factor_bloat_cases = [
        f"{decision.entity_id}: budget usage {decision.budget_usage}"
        for decision in prescreens.values()
        if sum(decision.budget_usage.values()) > 20
    ]
    false_trigger_cases = []
    if run_state.trigger_event_ids and not deltas:
        false_trigger_cases.append("Trigger wakeup produced no state deltas")
    state_transition_anomalies = [
        f"{delta.entity_id}: prescreen-only score entered {delta.to_bucket.value}"
        for delta in deltas
        if delta.score_snapshot.score_version == "score_v1_prescreen" and delta.to_bucket.value not in {"shadow_watch"}
    ]

    summary = (
        f"Run {run_state.run_id} produced {len(deltas)} deltas, "
        f"entry_hit_rate={metrics.entry_hit_rate:.2f}, "
        f"false_positive_trigger_rate={metrics.false_positive_trigger_rate:.2f}."
    )
    return PostMortemReport(
        report_id=make_id("pm"),
        run_id=run_state.run_id,
        metrics_id=metrics.metrics_id,
        wrong_judgment_cases=wrong_judgments,
        delayed_judgment_cases=delayed_judgments,
        factor_bloat_cases=factor_bloat_cases,
        false_trigger_cases=false_trigger_cases,
        state_transition_anomalies=state_transition_anomalies,
        summary=summary,
    )
