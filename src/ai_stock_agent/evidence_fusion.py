from __future__ import annotations

from datetime import UTC, datetime

from .models import (
    EvidenceChannelConfidence,
    EvidenceFusionSummary,
    FrozenQuantPacket,
    MarketContextSnapshot,
    TriggerEventRecord,
    WebResearchRecord,
)
from .utils import make_id


def _label_for_confidence(value: float) -> str:
    if value >= 0.82:
        return "high"
    if value >= 0.64:
        return "medium"
    if value >= 0.45:
        return "watch"
    return "fragile"


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, round(value, 4)))


def _official_bonus(count: int) -> float:
    return min(0.12, count * 0.04)


def _hard_data_channel(packet: FrozenQuantPacket) -> EvidenceChannelConfidence:
    manifest = packet.source_manifest
    coverage = [float(item.field_coverage) for item in manifest]
    avg_coverage = sum(coverage) / len(coverage) if coverage else 0.75
    freshness_score = max(0.2, 1 - min(packet.data_freshness_hours, 240) / 300)
    confidence = _clamp01(avg_coverage * 0.62 + freshness_score * 0.38)
    return EvidenceChannelConfidence(
        channel="hard_data",
        confidence_score=confidence,
        freshness_hours=float(packet.data_freshness_hours),
        source_count=len(manifest),
        source_names=sorted({item.source_name for item in manifest}),
        status="ok" if confidence >= 0.6 else "thin",
    )


def _web_channel(records: list[WebResearchRecord]) -> tuple[EvidenceChannelConfidence, int]:
    evidence_items = [item for record in records for item in record.evidence_items]
    if not evidence_items:
        return (
            EvidenceChannelConfidence(
                channel="web_research",
                confidence_score=0.0,
                freshness_hours=168.0,
                source_count=0,
                source_names=[],
                status="empty",
            ),
            0,
        )
    now = datetime.now(UTC)
    freshness_hours = min(
        max(0.0, (now - min(item.published_at for item in evidence_items)).total_seconds() / 3600),
        240.0,
    )
    avg_quality = sum(item.source_score for item in evidence_items) / len(evidence_items)
    avg_relevance = sum(item.relevance_score for item in evidence_items) / len(evidence_items)
    freshness_score = max(0.25, 1 - freshness_hours / 320)
    official_count = sum(1 for item in evidence_items if item.source_tier == "official")
    tier1_count = sum(1 for item in evidence_items if item.source_tier in {"official", "tier1_media"})
    confidence = _clamp01(
        avg_quality * 0.45
        + avg_relevance * 0.31
        + freshness_score * 0.16
        + _official_bonus(official_count)
        + min(0.06, tier1_count * 0.015)
    )
    return (
        EvidenceChannelConfidence(
            channel="web_research",
            confidence_score=confidence,
            freshness_hours=round(freshness_hours, 2),
            source_count=len({item.source_name for item in evidence_items}),
            source_names=sorted({item.source_name for item in evidence_items}),
            status="official_backed" if official_count > 0 else "ok" if confidence >= 0.58 else "thin",
        ),
        official_count,
    )


def _trigger_channel(events: list[TriggerEventRecord]) -> EvidenceChannelConfidence:
    if not events:
        return EvidenceChannelConfidence(
            channel="trigger_history",
            confidence_score=0.0,
            freshness_hours=168.0,
            source_count=0,
            source_names=[],
            status="empty",
        )
    now = datetime.now(UTC)
    freshest = min(max(0.0, (now - max(event.event_time for event in events)).total_seconds() / 3600), 240.0)
    avg_confidence = sum(event.trigger_confidence for event in events) / len(events)
    avg_quality = sum(event.source_quality_score for event in events) / len(events)
    official_like_count = sum(1 for event in events if event.source_quality_score >= 0.9)
    freshness_score = max(0.25, 1 - freshest / 240)
    confidence = _clamp01(
        avg_confidence * 0.47
        + avg_quality * 0.23
        + freshness_score * 0.22
        + _official_bonus(official_like_count)
    )
    return EvidenceChannelConfidence(
        channel="trigger_history",
        confidence_score=confidence,
        freshness_hours=round(freshest, 2),
        source_count=len(events),
        source_names=sorted({event.source_ref for event in events})[:6],
        status="official_backed" if official_like_count > 0 else "ok" if confidence >= 0.58 else "thin",
    )


def build_entity_evidence_summary(
    *,
    run_id: str,
    entity_id: str,
    packet: FrozenQuantPacket,
    web_records: list[WebResearchRecord],
    trigger_events: list[TriggerEventRecord],
) -> EvidenceFusionSummary:
    hard_data = _hard_data_channel(packet)
    web_research, official_count = _web_channel([record for record in web_records if record.entity_id == entity_id])
    trigger_history = _trigger_channel([event for event in trigger_events if entity_id in event.impacted_entities])
    overall = _clamp01(
        hard_data.confidence_score * 0.48
        + web_research.confidence_score * 0.3
        + trigger_history.confidence_score * 0.18
        + _official_bonus(official_count)
        + min(0.04, len(set(web_research.source_names)) * 0.01)
    )
    diversity = len(set(hard_data.source_names + web_research.source_names + trigger_history.source_names))
    label = _label_for_confidence(overall)
    return EvidenceFusionSummary(
        summary_id=make_id("evidence"),
        run_id=run_id,
        entity_id=entity_id,
        scope="entity",
        overall_confidence=overall,
        hard_data_confidence=hard_data.confidence_score,
        web_confidence=web_research.confidence_score,
        official_evidence_count=official_count,
        source_diversity=diversity,
        confidence_label=label,
        channels=[hard_data, web_research, trigger_history],
        summary=(
            f"Entity evidence is {label} with hard-data {hard_data.confidence_score:.2f}, "
            f"web {web_research.confidence_score:.2f}, trigger {trigger_history.confidence_score:.2f}, "
            f"and {official_count} official web references."
        ),
    )


def build_market_evidence_summary(
    *,
    run_id: str,
    market_context: MarketContextSnapshot,
    web_records: list[WebResearchRecord],
    trigger_events: list[TriggerEventRecord],
) -> EvidenceFusionSummary:
    health_records = market_context.source_health
    health_confidence = (
        sum(record.field_completeness * (0.85 if record.status == "healthy" else 0.55) for record in health_records) / len(health_records)
        if health_records
        else 0.6
    )
    hard_channel = EvidenceChannelConfidence(
        channel="market_context",
        confidence_score=_clamp01(health_confidence),
        freshness_hours=round(sum(record.latency_hours for record in health_records) / len(health_records), 2) if health_records else 24.0,
        source_count=len(health_records),
        source_names=[record.primary_source for record in health_records],
        status=market_context.context_status,
    )
    web_channel, official_count = _web_channel([record for record in web_records if record.entity_id is None])
    trigger_channel = _trigger_channel([event for event in trigger_events if not event.impacted_entities])
    overall = _clamp01(
        hard_channel.confidence_score * 0.52
        + web_channel.confidence_score * 0.24
        + trigger_channel.confidence_score * 0.18
        + _official_bonus(official_count)
        + min(0.04, len(set(web_channel.source_names)) * 0.01)
    )
    diversity = len(set(hard_channel.source_names + web_channel.source_names + trigger_channel.source_names))
    label = _label_for_confidence(overall)
    return EvidenceFusionSummary(
        summary_id=make_id("evidence"),
        run_id=run_id,
        entity_id=None,
        scope="market",
        overall_confidence=overall,
        hard_data_confidence=hard_channel.confidence_score,
        web_confidence=web_channel.confidence_score,
        official_evidence_count=official_count,
        source_diversity=diversity,
        confidence_label=label,
        channels=[hard_channel, web_channel, trigger_channel],
        summary=(
            f"Market evidence is {label} with context {hard_channel.confidence_score:.2f}, "
            f"macro web {web_channel.confidence_score:.2f}, and trigger stream {trigger_channel.confidence_score:.2f}."
        ),
    )
