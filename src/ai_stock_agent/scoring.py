from __future__ import annotations

from datetime import UTC, date, datetime

from .data_adapters import DataSourceManager
from .models import (
    Bucket,
    CompanyStateRecord,
    CurrentRoute,
    EvidenceFusionSummary,
    EntityProfile,
    FrozenQuantPacket,
    KnowledgeContextRecord,
    MarketContextSnapshot,
    PrescreenDecision,
    ScoreCard,
    SourceType,
    ThemeDecomposition,
    ThesisStatus,
    TransitionError,
    WeightCalibrationPolicy,
)
from .utils import clamp, make_id, model_hash


ALLOWED_BUCKET_TRANSITIONS: set[tuple[Bucket, Bucket]] = {
    (Bucket.SECONDARY_CANDIDATES, Bucket.CORE_TRACKING),
    (Bucket.HIGH_BETA_WATCH, Bucket.SECONDARY_CANDIDATES),
    (Bucket.SHADOW_WATCH, Bucket.SECONDARY_CANDIDATES),
    (Bucket.CORE_TRACKING, Bucket.SECONDARY_CANDIDATES),
    (Bucket.SECONDARY_CANDIDATES, Bucket.HIGH_BETA_WATCH),
    (Bucket.CORE_TRACKING, Bucket.ARCHIVED),
    (Bucket.SECONDARY_CANDIDATES, Bucket.ARCHIVED),
    (Bucket.HIGH_BETA_WATCH, Bucket.ARCHIVED),
    (Bucket.SHADOW_WATCH, Bucket.ARCHIVED),
    (Bucket.ARCHIVED, Bucket.SHADOW_WATCH),
}

ALLOWED_THESIS_TRANSITIONS: set[tuple[ThesisStatus, ThesisStatus]] = {
    (ThesisStatus.FORMING, ThesisStatus.VALIDATED),
    (ThesisStatus.FORMING, ThesisStatus.FRAGILE),
    (ThesisStatus.VALIDATED, ThesisStatus.ACCELERATING),
    (ThesisStatus.VALIDATED, ThesisStatus.FRAGILE),
    (ThesisStatus.VALIDATED, ThesisStatus.BROKEN),
    (ThesisStatus.ACCELERATING, ThesisStatus.VALIDATED),
    (ThesisStatus.ACCELERATING, ThesisStatus.FRAGILE),
    (ThesisStatus.ACCELERATING, ThesisStatus.BROKEN),
    (ThesisStatus.FRAGILE, ThesisStatus.VALIDATED),
    (ThesisStatus.FRAGILE, ThesisStatus.BROKEN),
    (ThesisStatus.FRAGILE, ThesisStatus.RETIRED),
    (ThesisStatus.BROKEN, ThesisStatus.RETIRED),
}


def build_frozen_packet(entity: EntityProfile, data_source_manager: DataSourceManager) -> FrozenQuantPacket:
    filing_payload = data_source_manager.get_entity_filing_metrics(entity)
    source_manifest = filing_payload.source_manifest + [
        {
            "source_name": "internal_quant_pipeline",
            "source_type": SourceType.PIPELINE,
            "as_of": datetime.now(UTC),
            "field_coverage": 0.95,
        }
    ]
    filing_values = {
        "base_quality": filing_payload.values.get("base_quality", entity.base_quality),
        "industry_position_score": filing_payload.values.get("industry_position_score", entity.industry_position_score),
        "macro_alignment_score": filing_payload.values.get("macro_alignment_score", entity.macro_alignment_score),
        "valuation_score": filing_payload.values.get("valuation_score", entity.valuation_score),
        "catalyst_score": filing_payload.values.get("catalyst_score", entity.catalyst_score),
        "risk_penalty": filing_payload.values.get("risk_penalty", entity.risk_penalty),
        "composite_tradability": filing_payload.values.get(
            "composite_tradability",
            round((entity.liquidity_score + entity.tradability_score) / 2, 2),
        ),
        "data_freshness_hours": filing_payload.values.get("data_freshness_hours", min(entity.evidence_freshness_days * 12, 240)),
    }
    packet = FrozenQuantPacket(
        as_of_date=date.today(),
        data_freshness_hours=int(filing_values["data_freshness_hours"]),
        raw_metrics={
            "base_quality": filing_values["base_quality"],
            "industry_position_score": filing_values["industry_position_score"],
            "macro_alignment_score": filing_values["macro_alignment_score"],
            "valuation_score": filing_values["valuation_score"],
            "catalyst_score": filing_values["catalyst_score"],
            "risk_penalty": filing_values["risk_penalty"],
        },
        derived_metrics={
            "evidence_freshness_days": entity.evidence_freshness_days,
            "composite_tradability": filing_values["composite_tradability"],
            "data_domain_status": filing_payload.health_record.status,
        },
        peer_percentiles={
            "quality_pct": round(entity.base_quality / 100, 4),
            "valuation_pct": round(entity.valuation_score / 100, 4),
        },
        valuation_snapshot={"relative_score": entity.valuation_score},
        tradability_snapshot={
            "liquidity_score": entity.liquidity_score,
            "info_score": entity.info_score,
            "tradability_score": entity.tradability_score,
            "provider_status": filing_payload.health_record.status,
        },
        source_manifest=source_manifest,
        packet_hash="pending",
    )
    return packet.model_copy(update={"packet_hash": model_hash(packet.model_dump(mode="json"))})


def compute_staleness_penalty(packet: FrozenQuantPacket, policy: WeightCalibrationPolicy) -> tuple[float, str]:
    freshness_days = int(packet.derived_metrics["evidence_freshness_days"])
    penalty = min(freshness_days * policy.stale_penalty_per_day, 35.0)
    if freshness_days <= 10:
        level = "fresh"
    elif freshness_days <= 30:
        level = "aging"
    else:
        level = "stale"
    return round(penalty, 2), level


def compute_scorecard(
    entity: EntityProfile,
    packet: FrozenQuantPacket,
    policy: WeightCalibrationPolicy,
    is_challenger: bool,
    theme: ThemeDecomposition | None = None,
    market_context: MarketContextSnapshot | None = None,
    prescreen: PrescreenDecision | None = None,
    evidence_summary: EvidenceFusionSummary | None = None,
    knowledge_context: KnowledgeContextRecord | None = None,
) -> ScoreCard:
    macro_alignment = entity.macro_alignment_score
    context_status = "healthy"
    regime_note = "unknown"
    advance_ratio = None
    breadth_thrust = None
    median_move = None
    if market_context is not None:
        regime_note = market_context.regime
        context_status = market_context.context_status
        indicator_map = {indicator.label: indicator.value for indicator in market_context.indicators}
        advance_ratio = indicator_map.get("Advance ratio")
        breadth_thrust = indicator_map.get("Breadth thrust")
        median_move = indicator_map.get("Median move")
        if market_context.regime == "risk_off":
            macro_alignment -= 6 if entity.risk_penalty > 40 else 2
        elif market_context.regime == "balanced_growth":
            macro_alignment += 4 if "ai_compute" in entity.active_factor_exposures else 1
        elif market_context.regime == "selective_risk_on":
            macro_alignment += 2 if entity.risk_penalty <= 45 else 0
        macro_alignment += min(6, round(market_context.regime_confidence * 4, 2))
        if advance_ratio is not None:
            if advance_ratio < 45:
                macro_alignment -= 4
            elif advance_ratio >= 55:
                macro_alignment += 2 if "ai_compute" in entity.active_factor_exposures else 1
        if breadth_thrust is not None:
            if breadth_thrust >= 65:
                macro_alignment += 2 if entity.risk_penalty <= 45 else 1
            elif breadth_thrust < 45:
                macro_alignment -= 4 if entity.risk_penalty > 35 else 2
        if median_move is not None and median_move < -0.6:
            macro_alignment -= 3
        if context_status == "degraded":
            macro_alignment -= 3
        elif context_status == "data_blocked":
            macro_alignment -= 8

    theme_boost = 0.0
    if theme is not None:
        linked_factors = {factor for item in theme.theme_slices for factor in item.linked_factors}
        overlap = len(linked_factors.intersection(entity.active_factor_exposures))
        theme_boost = min(6.0, overlap * 1.5)
    prescreen_multiplier = prescreen.confidence_multiplier if prescreen is not None else 1.0
    evidence_confidence = evidence_summary.overall_confidence if evidence_summary is not None else None
    knowledge_macro_overlay = 0.0
    knowledge_crowding_penalty = 0.0
    knowledge_principle_penalty = 0.0
    if evidence_summary is not None:
        if evidence_summary.overall_confidence < 0.5:
            macro_alignment -= 4
        elif evidence_summary.overall_confidence >= 0.8:
            macro_alignment += 2
        if evidence_summary.official_evidence_count > 0:
            macro_alignment += min(3, evidence_summary.official_evidence_count + 1)
    if knowledge_context is not None:
        knowledge_macro_overlay = (
            knowledge_context.macro_signal.score
            * knowledge_context.macro_signal.confidence
            * 10
            * policy.macro_cap
            / 100
        )
        macro_alignment += knowledge_macro_overlay
        knowledge_crowding_penalty = (
            abs(min(0.0, knowledge_context.consensus_signal.score))
            * knowledge_context.consensus_signal.confidence
            * 16
        )
        if is_challenger:
            knowledge_crowding_penalty *= 1.15
        if "high_crowding" in knowledge_context.consensus_signal.risk_flags:
            knowledge_crowding_penalty += 2.0
        knowledge_principle_penalty = (
            abs(min(0.0, knowledge_context.principle_signal.score))
            * knowledge_context.principle_signal.confidence
            * 10
        )
        if "valuation_discipline" in knowledge_context.principle_signal.risk_flags and entity.valuation_score < 62:
            knowledge_principle_penalty += 1.5

    quality_component = (
        entity.base_quality * (policy.fundamental_cap / 100)
        + entity.industry_position_score * (policy.industry_cap / 100)
        + clamp(macro_alignment + theme_boost) * (policy.macro_cap / 100)
    )
    quality_component += entity.tradability_score * 0.1
    current_quality_score = clamp((quality_component / 0.9) * min(1.1, prescreen_multiplier))

    freshness_penalty, _ = compute_staleness_penalty(packet, policy)
    evidence_boost = max(0.0, 25 - entity.evidence_freshness_days * 0.6)
    catalyst_impact = entity.catalyst_score * 0.4
    risk_drag = entity.risk_penalty * 0.3
    trajectory_score = clamp(45 + evidence_boost + catalyst_impact - risk_drag)
    trajectory_score = clamp(trajectory_score * min(1.08, prescreen_multiplier))
    if evidence_summary is not None:
        if evidence_summary.overall_confidence >= 0.8:
            trajectory_score = clamp(trajectory_score + 2)
        elif evidence_summary.overall_confidence < 0.48:
            trajectory_score = clamp(trajectory_score - 5)
    if knowledge_crowding_penalty:
        trajectory_score = clamp(trajectory_score - knowledge_crowding_penalty)
    if breadth_thrust is not None:
        if breadth_thrust < 40 and is_challenger:
            trajectory_score = clamp(trajectory_score - 4)
        elif breadth_thrust >= 68 and is_challenger:
            trajectory_score = clamp(trajectory_score + 3)

    valuation_component = entity.valuation_score * (policy.valuation_cap / 100)
    freshness_component = max(0.0, 15 - freshness_penalty / 2)
    risk_penalty_component = entity.risk_penalty * 0.35

    breakthrough_bonus = 0.0
    if is_challenger and current_quality_score >= policy.quality_floor_for_bonus:
        if entity.catalyst_score >= 65 and entity.valuation_score >= 60 and entity.risk_penalty <= 45:
            breakthrough_bonus = min(policy.max_breakthrough_bonus, 8 + entity.catalyst_score * 0.08)
            if market_context is not None and market_context.regime == "risk_off":
                breakthrough_bonus = max(0.0, breakthrough_bonus - 3)

    retention_priority_score = clamp(
        current_quality_score * 0.35
        + trajectory_score * 0.25
        + valuation_component
        + freshness_component
        + breakthrough_bonus
        - risk_penalty_component
    )
    if market_context is not None and market_context.context_status == "data_blocked":
        retention_priority_score = clamp(retention_priority_score - 10)
    if prescreen is not None and not prescreen.passed:
        retention_priority_score = clamp(retention_priority_score - policy.prescreen_data_block_penalty)
    if evidence_summary is not None and evidence_summary.overall_confidence < 0.45:
        retention_priority_score = clamp(retention_priority_score - 6)
    if knowledge_crowding_penalty:
        retention_priority_score = clamp(retention_priority_score - knowledge_crowding_penalty)
    if knowledge_principle_penalty:
        retention_priority_score = clamp(retention_priority_score - knowledge_principle_penalty)

    return ScoreCard(
        entity_id=entity.entity_id,
        current_quality_score=round(current_quality_score, 2),
        trajectory_score=round(trajectory_score, 2),
        retention_priority_score=round(retention_priority_score, 2),
        valuation_component=round(valuation_component, 2),
        freshness_component=round(freshness_component, 2),
        breakthrough_bonus_score=round(breakthrough_bonus, 2),
        risk_penalty_component=round(risk_penalty_component, 2),
        score_version="score_v1",
        notes=[
            f"staleness_penalty={freshness_penalty:.2f}",
            f"is_challenger={str(is_challenger).lower()}",
            f"regime={regime_note}",
            f"context_status={context_status}",
            f"advance_ratio={advance_ratio:.2f}" if advance_ratio is not None else "advance_ratio=na",
            f"breadth_thrust={breadth_thrust:.2f}" if breadth_thrust is not None else "breadth_thrust=na",
            f"theme_boost={theme_boost:.2f}",
            f"evidence_confidence={evidence_confidence:.2f}" if evidence_confidence is not None else "evidence_confidence=na",
            f"evidence_label={evidence_summary.confidence_label if evidence_summary is not None else 'na'}",
            f"official_evidence={evidence_summary.official_evidence_count if evidence_summary is not None else 'na'}",
            f"knowledge_macro_overlay={knowledge_macro_overlay:.2f}",
            f"knowledge_crowding_penalty={knowledge_crowding_penalty:.2f}",
            f"knowledge_principle_penalty={knowledge_principle_penalty:.2f}",
            f"knowledge_consensus_mode={knowledge_context.consensus_signal.action_mode if knowledge_context is not None else 'na'}",
            f"knowledge_principle_mode={knowledge_context.principle_signal.action_mode if knowledge_context is not None else 'na'}",
            f"prescreen_multiplier={prescreen_multiplier:.2f}",
            f"prescreen_decision={prescreen.decision if prescreen is not None else 'na'}",
        ],
    )


def infer_thesis_status(scorecard: ScoreCard) -> ThesisStatus:
    if scorecard.current_quality_score >= 75 and scorecard.trajectory_score >= 70:
        return ThesisStatus.ACCELERATING
    if scorecard.current_quality_score >= 65:
        return ThesisStatus.VALIDATED
    if scorecard.retention_priority_score >= 45:
        return ThesisStatus.FRAGILE
    return ThesisStatus.BROKEN


def validate_bucket_transition(old: Bucket, new: Bucket) -> None:
    if old == new:
        return
    if (old, new) not in ALLOWED_BUCKET_TRANSITIONS:
        raise TransitionError(f"Illegal bucket transition: {old} -> {new}")


def validate_thesis_transition(old: ThesisStatus, new: ThesisStatus) -> None:
    if old == new:
        return
    if (old, new) not in ALLOWED_THESIS_TRANSITIONS:
        raise TransitionError(f"Illegal thesis transition: {old} -> {new}")


def apply_score_to_company(
    company: CompanyStateRecord,
    scorecard: ScoreCard,
    packet: FrozenQuantPacket,
    market_context: MarketContextSnapshot | None = None,
) -> CompanyStateRecord:
    freshness_penalty = 30 - max(0.0, scorecard.freshness_component * 2)
    staleness_level = "fresh" if freshness_penalty <= 10 else "aging" if freshness_penalty <= 20 else "stale"
    return company.model_copy(
        update={
            "current_quality_score": scorecard.current_quality_score,
            "trajectory_score": scorecard.trajectory_score,
            "retention_priority_score": scorecard.retention_priority_score,
            "staleness_penalty": round(freshness_penalty, 2),
            "staleness_level": staleness_level,
            "score_version": scorecard.score_version,
            "latest_packet_hash": packet.packet_hash,
            "macro_context_tag": market_context.regime if market_context is not None else company.macro_context_tag,
        }
    )


def make_new_company_state(entity: EntityProfile, bucket: Bucket, route: CurrentRoute) -> CompanyStateRecord:
    today = date.today()
    return CompanyStateRecord(
        entity_id=entity.entity_id,
        ticker=entity.ticker,
        company_name=entity.company_name,
        current_bucket=bucket,
        current_route=route,
        thesis_status=ThesisStatus.FORMING,
        last_primary_evidence_date=today,
        last_confirmed_date=today,
        active_factor_exposures=entity.active_factor_exposures,
        review_chain_id=make_id("review"),
    )


def build_prescreen_shadow_scorecard(entity: EntityProfile, prescreen: PrescreenDecision) -> ScoreCard:
    quality = clamp(entity.base_quality * 0.72)
    trajectory = clamp(max(20.0, 48 - entity.risk_penalty * 0.25))
    retention = clamp(
        quality * 0.3
        + trajectory * 0.2
        + entity.valuation_score * 0.08
        - entity.risk_penalty * 0.25
        - len(prescreen.blocked_reasons) * 4
    )
    return ScoreCard(
        entity_id=entity.entity_id,
        current_quality_score=round(quality, 2),
        trajectory_score=round(trajectory, 2),
        retention_priority_score=round(retention, 2),
        valuation_component=round(entity.valuation_score * 0.08, 2),
        freshness_component=0.0,
        breakthrough_bonus_score=0.0,
        risk_penalty_component=round(entity.risk_penalty * 0.25, 2),
        score_version="score_v1_prescreen",
        notes=[
            "prescreen_only=true",
            f"prescreen_decision={prescreen.decision}",
            f"blocked_reasons={','.join(prescreen.blocked_reasons) or 'none'}",
        ],
    )
