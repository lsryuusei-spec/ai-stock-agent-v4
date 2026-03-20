from __future__ import annotations

from .models import (
    CurrentRoute,
    EntityProfile,
    FactorRegistryItem,
    MarketContextSnapshot,
    PrescreenDecision,
    ThemeDecomposition,
    WeightCalibrationPolicy,
)
from .utils import make_id


def build_default_factor_registry() -> list[FactorRegistryItem]:
    return [
        FactorRegistryItem(
            factor_id="factor_ai_compute",
            factor_name="AI compute demand",
            factor_type="score_additive",
            scope="global_factor_library",
            evidence_requirements=["quant_verified", "filing_based"],
            budget_cap=12,
            multiplier_rule="quant_verified",
            cluster_id="demand",
            exclusion_rules=[],
            shadow_mode=False,
            shadow_cycles_required=0,
            promotion_criteria="Demand indicators remain above threshold for 2 cycles",
            retirement_criteria="Demand indicators fall below threshold for 3 cycles",
        ),
        FactorRegistryItem(
            factor_id="factor_datacenter",
            factor_name="Datacenter capex buildout",
            factor_type="score_additive",
            scope="market_pack",
            evidence_requirements=["filing_based", "management_guidance"],
            budget_cap=10,
            multiplier_rule="filing_based",
            cluster_id="capex",
            exclusion_rules=[],
            shadow_mode=False,
            shadow_cycles_required=0,
            promotion_criteria="Capex pipeline broadens across peers",
            retirement_criteria="Orders and backlog normalize materially",
        ),
        FactorRegistryItem(
            factor_id="factor_foundry",
            factor_name="Foundry leverage",
            factor_type="score_additive",
            scope="sector_pack",
            evidence_requirements=["filing_based"],
            budget_cap=9,
            multiplier_rule="filing_based",
            cluster_id="supply_chain",
            exclusion_rules=[],
            shadow_mode=False,
            shadow_cycles_required=0,
            promotion_criteria="Utilization and pricing both improve",
            retirement_criteria="Utilization turns down for multiple cycles",
        ),
        FactorRegistryItem(
            factor_id="factor_quality",
            factor_name="Resilience premium",
            factor_type="multiplier_only",
            scope="global_factor_library",
            evidence_requirements=["quant_verified"],
            budget_cap=8,
            multiplier_rule="quant_verified",
            cluster_id="quality",
            exclusion_rules=[],
            shadow_mode=False,
            shadow_cycles_required=0,
            promotion_criteria="Balance sheet and execution remain top quartile",
            retirement_criteria="Operational misses or leverage weaken resilience thesis",
        ),
        FactorRegistryItem(
            factor_id="factor_beta_guard",
            factor_name="High beta risk gate",
            factor_type="veto_only",
            scope="market_pack",
            evidence_requirements=["news_sentiment"],
            budget_cap=0,
            multiplier_rule="news_sentiment",
            cluster_id="risk",
            exclusion_rules=[],
            shadow_mode=False,
            shadow_cycles_required=0,
            promotion_criteria="Risk regime broadens",
            retirement_criteria="Volatility remains elevated",
        ),
    ]


FACTOR_EXPOSURE_MAP: dict[str, str] = {
    "ai_compute": "factor_ai_compute",
    "ai_server": "factor_ai_compute",
    "datacenter": "factor_datacenter",
    "foundry": "factor_foundry",
    "quality": "factor_quality",
    "defensive_growth": "factor_quality",
}


def _multiplier_from_rule(rule: str, policy: WeightCalibrationPolicy) -> float:
    if rule == "quant_verified":
        return policy.quant_verified_multiplier
    if rule == "filing_based":
        return policy.filing_based_multiplier
    if rule == "management_guidance":
        return policy.management_guidance_multiplier
    if rule == "news_sentiment":
        return policy.news_sentiment_multiplier
    return 1.0


def prescreen_challenger(
    entity: EntityProfile,
    theme: ThemeDecomposition,
    market_context: MarketContextSnapshot,
    factor_registry: list[FactorRegistryItem],
    policy: WeightCalibrationPolicy,
) -> PrescreenDecision:
    registry_map = {item.factor_id: item for item in factor_registry}
    theme_factors = {factor for item in theme.theme_slices for factor in item.linked_factors}
    relevant_exposures = sorted(set(entity.active_factor_exposures).intersection(theme_factors))
    passed_factors = []
    budget_usage: dict[str, float] = {}
    confidence_multiplier = 1.0
    blocked_reasons: list[str] = []

    for exposure in relevant_exposures:
        factor_id = FACTOR_EXPOSURE_MAP.get(exposure)
        if factor_id is None:
            continue
        factor = registry_map.get(factor_id)
        if factor is None:
            continue
        passed_factors.append(factor.factor_name)
        budget_usage[factor.cluster_id] = budget_usage.get(factor.cluster_id, 0.0) + factor.budget_cap
        confidence_multiplier *= _multiplier_from_rule(factor.multiplier_rule, policy)

    if entity.info_score < policy.prescreen_min_info_score:
        blocked_reasons.append("insufficient_information")
    if entity.liquidity_score < policy.prescreen_min_liquidity_score:
        blocked_reasons.append("liquidity_below_threshold")
    if entity.risk_penalty > policy.prescreen_max_risk_penalty:
        blocked_reasons.append("risk_penalty_too_high")
    if len(relevant_exposures) < policy.prescreen_min_factor_overlap:
        blocked_reasons.append("theme_factor_overlap_too_low")
    if market_context.context_status == "data_blocked":
        blocked_reasons.append("market_context_blocked")
        confidence_multiplier = max(0.4, confidence_multiplier - 0.4)
    elif market_context.context_status == "degraded":
        confidence_multiplier *= 0.85

    if market_context.regime == "risk_off" and entity.risk_penalty > 45:
        blocked_reasons.append("risk_off_rejection")

    total_budget = sum(budget_usage.values())
    if total_budget > (policy.macro_cap + policy.catalyst_cap + policy.valuation_cap):
        blocked_reasons.append("factor_budget_exceeded")

    passed = len(blocked_reasons) == 0
    if passed:
        decision = "deep_score"
        eligible_route = CurrentRoute.CHALLENGER_SCAN
    elif "market_context_blocked" in blocked_reasons:
        decision = "defer"
        eligible_route = CurrentRoute.SHADOW_OBSERVATION
    else:
        decision = "shadow_watch"
        eligible_route = CurrentRoute.SHADOW_OBSERVATION

    return PrescreenDecision(
        prescreen_id=make_id("prescreen"),
        entity_id=entity.entity_id,
        passed=passed,
        decision=decision,
        eligible_route=eligible_route,
        factor_overlap_count=len(relevant_exposures),
        passed_factors=passed_factors,
        blocked_reasons=blocked_reasons,
        confidence_multiplier=round(confidence_multiplier, 4),
        budget_usage=budget_usage,
        context_status=market_context.context_status,
    )
