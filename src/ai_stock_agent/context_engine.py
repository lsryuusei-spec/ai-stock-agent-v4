from __future__ import annotations

from datetime import UTC, datetime

from .data_adapters import DataSourceManager
from .models import (
    EventType,
    KnowledgeContextRecord,
    MarketContextSnapshot,
    MarketIndicator,
    ThemeDecomposition,
    ThemeSlice,
    TriggerEventRecord,
    ScenarioNode,
)
from .utils import make_id, model_hash


KEYWORD_FACTOR_MAP: dict[str, tuple[str, str, list[str]]] = {
    "ai": ("AI demand", "Demand acceleration from AI infrastructure spending", ["ai_compute", "ai_server"]),
    "infrastructure": ("Infrastructure buildout", "Capex cycle remains central to theme execution", ["datacenter", "foundry"]),
    "resilience": ("Resilience premium", "Preference shifts toward durable balance sheets and execution certainty", ["quality", "defensive_growth"]),
    "rates": ("Rate sensitivity", "Financing and valuation remain linked to rate path", ["duration", "valuation"]),
    "liquidity": ("Liquidity regime", "Risk appetite depends on market liquidity breadth", ["beta", "funding"]),
}


def decompose_theme(macro_theme: str, trigger_events: list[TriggerEventRecord]) -> ThemeDecomposition:
    lowered = macro_theme.lower()
    slices: list[ThemeSlice] = []
    matched_terms: list[str] = []
    for keyword, (label, rationale, factors) in KEYWORD_FACTOR_MAP.items():
        if keyword in lowered:
            matched_terms.append(keyword)
            slices.append(
                ThemeSlice(
                    slice_id=make_id("slice"),
                    label=label,
                    weight=0.0,
                    rationale=rationale,
                    linked_factors=factors,
                )
            )

    if not slices:
        slices = [
            ThemeSlice(
                slice_id=make_id("slice"),
                label="Core macro theme",
                weight=1.0,
                rationale="Fallback theme slice used when no specialized keyword matches",
                linked_factors=["general_macro"],
            )
        ]
    else:
        base_weight = round(1 / len(slices), 2)
        remainder = round(1 - base_weight * len(slices), 2)
        for index, item in enumerate(slices):
            item.weight = base_weight + (remainder if index == 0 else 0.0)

    stress_signal = sum(1 for event in trigger_events if event.event_type in {EventType.MACRO, EventType.PRICE_BREAK, EventType.NEWS})
    positive_signal = sum(1 for event in trigger_events if event.event_type in {EventType.EARNINGS, EventType.GUIDANCE})
    dominant_regime = "risk_off" if stress_signal > positive_signal else "balanced_growth"

    scenario_tree = [
        ScenarioNode(
            scenario_id=make_id("scenario"),
            name="bull",
            probability=0.25 if dominant_regime == "risk_off" else 0.35,
            key_drivers=["AI capex beats expectations", "Rates remain range-bound"],
            implications=["High-beta challengers gain room", "Valuation compression risk fades"],
        ),
        ScenarioNode(
            scenario_id=make_id("scenario"),
            name="base",
            probability=0.5,
            key_drivers=["Spend remains selective", "Leaders keep execution premium"],
            implications=["Core tracking favors quality leaders", "Secondary pool rotates on evidence freshness"],
        ),
        ScenarioNode(
            scenario_id=make_id("scenario"),
            name="bear",
            probability=0.25 if dominant_regime != "risk_off" else 0.35,
            key_drivers=["Macro growth softens", "Risk appetite narrows"],
            implications=["Shadow watch expands", "Breakthrough bonus should be harder to earn"],
        ),
    ]
    summary = f"Theme '{macro_theme}' decomposed into {len(slices)} slices under {dominant_regime} regime."
    decomposition = ThemeDecomposition(
        decomposition_id=make_id("theme"),
        macro_theme=macro_theme,
        theme_slices=slices,
        scenario_tree=scenario_tree,
        dominant_regime=dominant_regime,
        summary=summary,
        decomposition_hash="pending",
    )
    return decomposition.model_copy(
        update={"decomposition_hash": model_hash(decomposition.model_dump(mode="json"))}
    )


def build_market_context(
    macro_theme: str,
    decomposition: ThemeDecomposition,
    trigger_events: list[TriggerEventRecord],
    data_source_manager: DataSourceManager,
    knowledge_context: KnowledgeContextRecord | None = None,
) -> MarketContextSnapshot:
    now = datetime.now(UTC)
    macro_payload = data_source_manager.get_macro_prices(trigger_events)
    breadth_payload = data_source_manager.get_market_breadth(trigger_events)
    sector_payload = data_source_manager.get_sector_signals(trigger_events)

    yield_level = macro_payload.values["ust_10y"]
    liquidity_breadth = breadth_payload.values["liquidity_breadth"]
    advance_ratio = breadth_payload.values.get("advance_ratio", round(liquidity_breadth / 100, 4))
    active_turnover_ratio = breadth_payload.values.get("active_turnover_ratio", 0.85)
    median_change_pct = breadth_payload.values.get("median_change_pct", round((liquidity_breadth - 50) / 12, 3))
    breadth_thrust = breadth_payload.values.get("breadth_thrust", round(liquidity_breadth, 2))
    gainers_count = int(breadth_payload.values.get("gainers_count", 0))
    losers_count = int(breadth_payload.values.get("losers_count", 0))
    ai_capex_momentum = sector_payload.values["ai_capex_momentum"]
    volatility_regime = macro_payload.values["volatility_regime"]

    indicators = [
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="UST 10Y",
            value=yield_level,
            unit="percent",
            direction="up" if macro_payload.degraded else "flat",
            as_of=now,
            source_name=macro_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Liquidity breadth",
            value=liquidity_breadth,
            unit="score",
            direction="down" if breadth_payload.degraded or liquidity_breadth < 60 else "up",
            as_of=now,
            source_name=breadth_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Advance ratio",
            value=round(advance_ratio * 100, 2),
            unit="percent",
            direction="down" if advance_ratio < 0.48 else "up" if advance_ratio > 0.53 else "flat",
            as_of=now,
            source_name=breadth_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Median move",
            value=median_change_pct,
            unit="percent",
            direction="down" if median_change_pct < -0.3 else "up" if median_change_pct > 0.3 else "flat",
            as_of=now,
            source_name=breadth_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Breadth thrust",
            value=breadth_thrust,
            unit="score",
            direction="down" if breadth_thrust < 45 else "up" if breadth_thrust >= 60 else "flat",
            as_of=now,
            source_name=breadth_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Turnover participation",
            value=round(active_turnover_ratio * 100, 2),
            unit="percent",
            direction="down" if active_turnover_ratio < 0.65 else "up" if active_turnover_ratio >= 0.8 else "flat",
            as_of=now,
            source_name=breadth_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="AI capex momentum",
            value=ai_capex_momentum,
            unit="score",
            direction="down" if sector_payload.degraded else "up" if ai_capex_momentum >= 72 else "flat",
            as_of=now,
            source_name=sector_payload.source_manifest[0].source_name,
        ),
        MarketIndicator(
            indicator_id=make_id("ind"),
            label="Volatility regime",
            value=volatility_regime,
            unit="vix_like",
            direction="up" if volatility_regime >= 20 else "flat",
            as_of=now,
            source_name=macro_payload.source_manifest[0].source_name,
        ),
    ]

    source_health = [
        macro_payload.health_record,
        breadth_payload.health_record,
        sector_payload.health_record,
    ]

    context_status = "healthy"
    if any(record.status == "degraded" for record in source_health):
        context_status = "degraded"
    if any(record.field_completeness < 0.75 for record in source_health):
        context_status = "data_blocked"

    risk_signals = 0
    growth_signals = 0
    if volatility_regime >= 22:
        risk_signals += 1
    if liquidity_breadth < 55:
        risk_signals += 1
    if advance_ratio < 0.48:
        risk_signals += 1
    if median_change_pct < -0.4:
        risk_signals += 1
    if ai_capex_momentum >= 72:
        growth_signals += 1
    if liquidity_breadth >= 62:
        growth_signals += 1
    if advance_ratio >= 0.53:
        growth_signals += 1
    if median_change_pct > 0.2:
        growth_signals += 1

    regime = decomposition.dominant_regime
    if risk_signals >= 2 and risk_signals > growth_signals:
        regime = "risk_off"
    elif growth_signals >= 3 and growth_signals >= risk_signals + 1:
        regime = "balanced_growth"
    elif growth_signals >= 2:
        regime = "selective_risk_on"

    signal_gap = abs(growth_signals - risk_signals)
    regime_confidence = 0.48 + min(signal_gap, 3) * 0.1
    if context_status == "degraded":
        regime_confidence -= 0.08
    elif context_status == "data_blocked":
        regime_confidence -= 0.16

    crowding_risk_score = 0.0
    principle_constraint_score = 0.0
    knowledge_overlay_summary = None
    if knowledge_context is not None:
        macro_shift = knowledge_context.macro_signal.score * knowledge_context.macro_signal.confidence
        crowding_risk_score = round(abs(min(0.0, knowledge_context.consensus_signal.score)), 2)
        principle_constraint_score = round(abs(min(0.0, knowledge_context.principle_signal.score)), 2)
        if macro_shift <= -0.12:
            regime = "risk_off"
        elif macro_shift >= 0.18 and regime != "risk_off":
            regime = "selective_risk_on" if growth_signals >= 1 else "balanced_growth"
        regime_confidence += abs(macro_shift) * 0.12
        regime_confidence -= crowding_risk_score * 0.08
        regime_confidence -= principle_constraint_score * 0.04
        knowledge_overlay_summary = knowledge_context.overall_summary

    regime_confidence = max(0.35, min(0.9, round(regime_confidence, 2)))

    scenario_bias = {node.name: node.probability for node in decomposition.scenario_tree}
    if regime == "risk_off":
        scenario_bias["bear"] = round(min(0.55, scenario_bias.get("bear", 0.25) + 0.1), 2)
        scenario_bias["bull"] = round(max(0.15, scenario_bias.get("bull", 0.25) - 0.08), 2)
    elif regime in {"balanced_growth", "selective_risk_on"}:
        scenario_bias["bull"] = round(min(0.48, scenario_bias.get("bull", 0.25) + 0.08), 2)
        scenario_bias["bear"] = round(max(0.18, scenario_bias.get("bear", 0.25) - 0.05), 2)
    scenario_bias["base"] = round(max(0.2, 1 - scenario_bias.get("bull", 0.25) - scenario_bias.get("bear", 0.25)), 2)

    context_summary = (
        f"{regime} regime with breadth {liquidity_breadth}, advance ratio {advance_ratio:.2f}, "
        f"median move {median_change_pct:.2f}, AI capex momentum {ai_capex_momentum}, "
        f"and breadth split {gainers_count}/{losers_count}."
    )
    if knowledge_overlay_summary:
        context_summary = f"{context_summary} {knowledge_overlay_summary}"
    snapshot = MarketContextSnapshot(
        context_id=make_id("ctx"),
        macro_theme=macro_theme,
        regime=regime,
        regime_confidence=regime_confidence,
        indicators=indicators,
        scenario_bias=scenario_bias,
        source_health=source_health,
        context_summary=context_summary,
        context_status=context_status,
        crowding_risk_score=crowding_risk_score,
        principle_constraint_score=principle_constraint_score,
        knowledge_overlay_summary=knowledge_overlay_summary,
        context_hash="pending",
    )
    return snapshot.model_copy(update={"context_hash": model_hash(snapshot.model_dump(mode="json"))})
