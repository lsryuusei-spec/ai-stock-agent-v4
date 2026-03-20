from __future__ import annotations

import importlib
import json
from datetime import date
from pathlib import Path
from typing import Any

from .hk_enrichment import enrich_hk_records
from .models import Bucket, CurrentRoute, EntityProfile, ResearchPoolState, UniverseState
from .runtime_env import read_env_value
from .scoring import make_new_company_state


CN_CODE_KEYS = ("code", "symbol", "ts_code", "\u4ee3\u7801", "\u8bc1\u5238\u4ee3\u7801", "浠ｇ爜", "璇佸埜浠ｇ爜")
NAME_KEYS = ("name", "\u4e2d\u6587\u540d\u79f0", "\u540d\u79f0", "company_name", "鍚嶇О", "涓枃鍚嶇О")
SECTOR_KEYS = (
    "sector",
    "industry",
    "raw_sector",
    "\u6240\u5c5e\u884c\u4e1a",
    "\u677f\u5757",
    "market",
    "鎵€灞炶涓?",
    "鏉垮潡",
)
MARKET_CAP_KEYS = (
    "market_cap",
    "total_mv",
    "marketValue",
    "\u603b\u5e02\u503c",
    "\u603b\u5e02\u503c(\u6e2f\u5143)",
    "鎬诲競鍊?",
)
AMOUNT_KEYS = ("amount", "turnover", "\u6210\u4ea4\u989d", "\u6210\u4ea4\u91d1\u989d", "鎴愪氦棰?", "鎴愪氦閲戦")
TURNOVER_RATE_KEYS = ("turnover_rate", "turnover_rate_f", "turnoverRatio", "\u6362\u624b\u7387", "鎹㈡墜鐜?")
VOLUME_RATIO_KEYS = ("volume_ratio", "volumeRatio", "\u91cf\u6bd4", "閲忔瘮")
PCT_CHG_KEYS = ("pct_chg", "changePercent", "\u6da8\u8dcc\u5e45", "娑ㄨ穼骞?")
PE_KEYS = ("pe", "pe_ttm", "\u5e02\u76c8\u7387", "甯傜泩鐜?鍔ㄦ€?")
PB_KEYS = ("pb", "pb_ratio", "\u5e02\u51c0\u7387", "甯傚噣鐜?")
AMPLITUDE_KEYS = ("amplitude", "\u632f\u5e45", "鎸箙")


DEFAULT_BUILD_CONFIGS: dict[str, dict[str, Any]] = {
    "cn": {
        "source_kind": "hybrid",
        "source_priority": ["tushare", "akshare"],
        "function": "stock_zh_a_spot",
        "max_entities": 24,
        "core_size": 3,
        "secondary_size": 3,
        "high_beta_size": 2,
        "candidate_pool_size": 48,
        "auth_env_var": "TUSHARE_TOKEN",
        "sample_ticker": "688981.SH",
    },
    "hk": {
        "source_kind": "hybrid",
        "source_priority": ["tushare", "akshare"],
        "function": "stock_hk_spot",
        "max_entities": 24,
        "core_size": 3,
        "secondary_size": 3,
        "high_beta_size": 2,
        "candidate_pool_size": 48,
        "hk_enrichment_max_symbols": 18,
        "auth_env_var": "TUSHARE_TOKEN",
        "sample_ticker": "00700.HK",
    },
}


EXPOSURE_KEYWORDS: dict[str, list[str]] = {
    "ai": ["ai_compute"],
    "cloud": ["cloud"],
    "server": ["ai_server"],
    "optical": ["optical_interconnect"],
    "foundry": ["foundry"],
    "chip": ["advanced_nodes"],
    "semiconductor": ["advanced_nodes"],
    "internet": ["platform_moat"],
    "platform": ["platform_moat"],
    "telecom": ["networking"],
    "datacenter": ["datacenter"],
    "robot": ["automation"],
    "network": ["networking"],
}


def _safe_number(value: Any, default: float = 0.0) -> float:
    if value in (None, "", "-", "--"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pick_value(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _market_cap_value(record: dict[str, Any]) -> float:
    return _safe_number(_pick_value(record, *MARKET_CAP_KEYS), 0.0)


def _candidate_priority(record: dict[str, Any]) -> tuple[float, float, float]:
    return (
        _market_cap_value(record),
        _safe_number(_pick_value(record, *AMOUNT_KEYS), 0.0),
        abs(_safe_number(_pick_value(record, *PCT_CHG_KEYS), 0.0)),
    )


def _load_build_config(market: str, config_path: str | None = None) -> dict[str, Any]:
    config = dict(DEFAULT_BUILD_CONFIGS[market])
    if config_path:
        path = Path(config_path)
        if path.exists():
            config.update(json.loads(path.read_text(encoding="utf-8")))
    return config


def _records_from_frame(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if hasattr(frame, "to_dict"):
        return [item for item in frame.to_dict("records") if isinstance(item, dict)]
    if isinstance(frame, list):
        return [item for item in frame if isinstance(item, dict)]
    if isinstance(frame, dict):
        return [frame]
    return []


def _normalize_code(raw_code: Any, market: str) -> str:
    text = str(raw_code or "").upper()
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(char for char in text if char.isdigit())
    if market == "hk":
        return digits.zfill(5) if digits else text
    return digits.zfill(6) if digits else text


def _record_code(record: dict[str, Any], market: str) -> str:
    return _normalize_code(_pick_value(record, *CN_CODE_KEYS), market)


def _merge_record_lists(record_lists: list[list[dict[str, Any]]], market: str) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for records in record_lists:
        for record in records:
            code = _record_code(record, market)
            if not code:
                continue
            target = merged.setdefault(code, {"code": code})
            for key, value in record.items():
                if value not in (None, "", "-", "--"):
                    target[key] = value
    return list(merged.values())


def _latest_tushare_trade_date(client: Any, sample_ticker: str, api_name: str) -> str:
    if not hasattr(client, api_name):
        raise ValueError(f"tushare api not available: {api_name}")
    frame = getattr(client, api_name)(ts_code=sample_ticker, limit=1)
    records = _records_from_frame(frame)
    if not records:
        raise ValueError(f"tushare api returned no rows: {api_name}")
    trade_date = _pick_value(records[0], "trade_date")
    if not trade_date:
        raise ValueError(f"tushare api missing trade_date: {api_name}")
    return str(trade_date)


def _load_tushare_records(market: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        tushare = importlib.import_module("tushare")
    except ImportError as exc:
        raise ValueError("tushare is not installed") from exc
    token = config.get("token") or read_env_value(str(config.get("auth_env_var", "TUSHARE_TOKEN")))
    if not token:
        raise ValueError("missing TUSHARE_TOKEN")
    client = tushare.pro_api(token)

    if market == "cn":
        latest_trade_date = _latest_tushare_trade_date(client, str(config.get("sample_ticker", "688981.SH")), "daily")
        base_records = _records_from_frame(
            client.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,industry,market,list_date",
            )
        )
        daily_basic_records = _records_from_frame(client.daily_basic(trade_date=latest_trade_date))
        daily_records = _records_from_frame(client.daily(trade_date=latest_trade_date))
        optional_records: list[list[dict[str, Any]]] = []
        if hasattr(client, "stock_company"):
            company_fields = str(
                config.get(
                    "stock_company_fields",
                    "ts_code,exchange,introduction,main_business,business_scope,website",
                )
            )
            for exchange in ("SSE", "SZSE", "BSE"):
                try:
                    optional_records.append(
                        _records_from_frame(client.stock_company(exchange=exchange, fields=company_fields))
                    )
                except Exception:
                    continue
        if hasattr(client, "moneyflow"):
            try:
                optional_records.append(_records_from_frame(client.moneyflow(trade_date=latest_trade_date)))
            except Exception:
                pass
        return _merge_record_lists([base_records, daily_basic_records, daily_records, *optional_records], market)

    latest_trade_date = _latest_tushare_trade_date(client, str(config.get("sample_ticker", "00700.HK")), "hk_daily")
    base_records = _records_from_frame(client.hk_basic())
    daily_records = _records_from_frame(client.hk_daily(trade_date=latest_trade_date))
    return _merge_record_lists([base_records, daily_records], market)


def _load_akshare_records(market: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        akshare = importlib.import_module("akshare")
    except ImportError as exc:
        raise ValueError("akshare is not installed") from exc
    configured = str(config.get("function", "stock_hk_spot" if market == "hk" else "stock_zh_a_spot"))
    candidates = [configured]
    if market == "hk":
        candidates.extend(["stock_hk_spot", "stock_hk_spot_em"])
    else:
        candidates.extend(["stock_zh_a_spot", "stock_zh_a_spot_em"])
    seen: set[str] = set()
    for function_name in candidates:
        if function_name in seen:
            continue
        seen.add(function_name)
        if not hasattr(akshare, function_name):
            continue
        try:
            frame = getattr(akshare, function_name)(**dict(config.get("call_kwargs", {})))
        except Exception:
            continue
        records = _records_from_frame(frame)
        if records:
            return records
    raise ValueError("no akshare snapshot function returned records")


def _load_source_records(market: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    source_kind = str(config.get("source_kind", "hybrid")).lower()
    if source_kind == "file":
        file_path = config.get("file_path")
        if not file_path:
            raise ValueError("file source requires file_path")
        path = Path(file_path)
        if not path.exists():
            raise ValueError(f"file source not found: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("records", payload.get(market, []))
        else:
            records = payload
        return [item for item in records if isinstance(item, dict)]
    if source_kind == "akshare":
        try:
            return _load_akshare_records(market, config)
        except Exception as exc:
            raise ValueError(f"akshare source failed: {exc}") from exc
    if source_kind == "tushare":
        try:
            return _load_tushare_records(market, config)
        except Exception as exc:
            raise ValueError(f"tushare source failed: {exc}") from exc
    if source_kind != "hybrid":
        raise ValueError(f"unsupported source_kind: {source_kind}")

    merged_records: list[dict[str, Any]] = []
    errors: list[str] = []
    for source in list(config.get("source_priority", ["tushare", "akshare"])):
        try:
            source_records = _load_tushare_records(market, config) if source == "tushare" else _load_akshare_records(market, config)
        except Exception as exc:
            errors.append(f"{source}:{exc}")
            continue
        if not merged_records:
            merged_records = source_records
        else:
            merged_records = _merge_record_lists([merged_records, source_records], market)
    if merged_records:
        return merged_records
    raise ValueError(f"hybrid source failed: {'; '.join(errors) if errors else 'no records'}")


def _normalize_market_ticker(code: str, market: str) -> str:
    digits = "".join(char for char in str(code) if char.isdigit())
    if market == "hk":
        return f"{digits.zfill(5)}.HK"
    if digits.startswith(("6", "9")):
        return f"{digits.zfill(6)}.SH"
    if digits.startswith("8"):
        return f"{digits.zfill(6)}.BJ"
    return f"{digits.zfill(6)}.SZ"


def _infer_exposures(name: str, sector: str) -> list[str]:
    lowered = f"{name} {sector}".lower()
    exposures: list[str] = []
    for keyword, mapped in EXPOSURE_KEYWORDS.items():
        if keyword in lowered:
            exposures.extend(mapped)
    if not exposures:
        exposures.append("general_macro")
    return sorted(set(exposures))


def _valuation_score(pe: float, pb: float) -> float:
    score = 70.0
    if pe > 0:
        score -= min(28, max(0, pe - 12) * 0.6)
    if pb > 0:
        score -= min(18, max(0, pb - 1.8) * 2.5)
    return max(10.0, min(95.0, round(score, 2)))


def _build_entity_profile(record: dict[str, Any], market: str, rank: int, total: int) -> EntityProfile | None:
    code = _pick_value(record, *CN_CODE_KEYS)
    name = _pick_value(record, *NAME_KEYS)
    if not code or not name:
        return None
    sector = str(_pick_value(record, *SECTOR_KEYS) or "Unknown")
    market_cap = _market_cap_value(record)
    amount = _safe_number(_pick_value(record, *AMOUNT_KEYS), 0.0)
    turnover_rate = _safe_number(_pick_value(record, *TURNOVER_RATE_KEYS), 0.0)
    volume_ratio = _safe_number(_pick_value(record, *VOLUME_RATIO_KEYS), 0.0)
    pct_chg = _safe_number(_pick_value(record, *PCT_CHG_KEYS), 0.0)
    pe = _safe_number(_pick_value(record, *PE_KEYS), 0.0)
    pb = _safe_number(_pick_value(record, *PB_KEYS), 0.0)
    amplitude = _safe_number(_pick_value(record, *AMPLITUDE_KEYS), abs(pct_chg) * 1.2)
    website = _pick_value(record, "website", "company_website", "web_url", "\u516c\u53f8\u7f51\u5740")
    business_summary = _pick_value(
        record,
        "business_summary",
        "description",
        "company_profile",
        "introduction",
        "main_business",
        "business_scope",
        "\u516c\u53f8\u4ecb\u7ecd",
        "\u4e3b\u8425\u4e1a\u52a1",
    )
    english_name = _pick_value(record, "english_name", "enname", "\u82f1\u6587\u540d\u79f0")

    ticker = _normalize_market_ticker(str(code), market)
    entity_id = f"{_normalize_code(code, market).lower()}_{market}"
    exposures = _infer_exposures(str(name), sector)
    percentile = 1 - (rank / max(total, 1))
    liquidity_score = max(25.0, min(98.0, round((amount / 1_000_000_000) * 18 + turnover_rate * 4.5, 2)))
    tradability_score = max(25.0, min(98.0, round(liquidity_score * 0.7 + volume_ratio * 12, 2)))
    info_score = max(55.0, min(96.0, round(68 + (10 if sector != "Unknown" else 0) + percentile * 18, 2)))
    base_quality = max(45.0, min(95.0, round(58 + percentile * 26 + min(10, market_cap / 500), 2)))
    industry_position = max(40.0, min(96.0, round(52 + percentile * 30 + min(8, market_cap / 700), 2)))
    macro_alignment = max(45.0, min(96.0, round(60 + len(exposures) * 7 + (4 if "ai_compute" in exposures else 0), 2)))
    catalyst_score = max(35.0, min(96.0, round(55 + abs(pct_chg) * 3 + volume_ratio * 5, 2)))
    risk_penalty = max(12.0, min(88.0, round(22 + amplitude * 2.2 + (8 if "st" in str(name).lower() else 0), 2)))

    return EntityProfile(
        entity_id=entity_id,
        ticker=ticker,
        company_name=str(name),
        sector=sector,
        market_cap=round(market_cap, 2),
        website=str(website).strip() if website not in (None, "") else None,
        business_summary=str(business_summary).strip() if business_summary not in (None, "") else None,
        english_name=str(english_name).strip() if english_name not in (None, "") else None,
        liquidity_score=liquidity_score,
        info_score=info_score,
        tradability_score=tradability_score,
        base_quality=base_quality,
        industry_position_score=industry_position,
        macro_alignment_score=macro_alignment,
        valuation_score=_valuation_score(pe, pb),
        catalyst_score=catalyst_score,
        risk_penalty=risk_penalty,
        evidence_freshness_days=1,
        active_factor_exposures=exposures,
    )


def _rank_entities(entities: list[EntityProfile]) -> list[EntityProfile]:
    def score(entity: EntityProfile) -> float:
        return (
            entity.base_quality * 0.32
            + entity.industry_position_score * 0.22
            + entity.macro_alignment_score * 0.18
            + entity.valuation_score * 0.12
            + entity.catalyst_score * 0.16
            - entity.risk_penalty * 0.14
        )

    return sorted(entities, key=score, reverse=True)


def build_initial_universe_bundle(
    *,
    market: str,
    universe_id: str | None = None,
    pool_id: str | None = None,
    config_path: str | None = None,
) -> tuple[UniverseState, ResearchPoolState, list]:
    normalized_market = market.lower()
    if normalized_market not in {"cn", "hk"}:
        raise ValueError("market must be cn or hk")
    config = _load_build_config(normalized_market, config_path)
    raw_records = _load_source_records(normalized_market, config)
    max_entities = int(config.get("max_entities", 24))
    candidate_pool_size = max(max_entities, int(config.get("candidate_pool_size", max_entities * 2)))
    candidate_records = sorted(raw_records, key=_candidate_priority, reverse=True)[:candidate_pool_size]
    if normalized_market == "hk":
        candidate_records = enrich_hk_records(
            candidate_records,
            max_symbols=int(config.get("hk_enrichment_max_symbols", min(candidate_pool_size, 18))),
        )
    ranked_by_cap = sorted(candidate_records, key=_market_cap_value, reverse=True)
    entities = [
        entity
        for index, record in enumerate(ranked_by_cap[:max_entities])
        if (entity := _build_entity_profile(record, normalized_market, index, min(len(ranked_by_cap), max_entities))) is not None
    ]
    entities = _rank_entities(entities)
    if not entities:
        raise ValueError("no entities could be built from source data")

    core_size = int(config.get("core_size", 3))
    secondary_size = int(config.get("secondary_size", 3))
    high_beta_size = int(config.get("high_beta_size", 2))

    core_entities = entities[:core_size]
    secondary_entities = entities[core_size : core_size + secondary_size]
    remainder = entities[core_size + secondary_size :]
    high_beta_entities = sorted(
        remainder,
        key=lambda item: (item.catalyst_score + item.risk_penalty),
        reverse=True,
    )[:high_beta_size]

    companies = [
        *[make_new_company_state(item, Bucket.CORE_TRACKING, CurrentRoute.INCUMBENT_REVIEW) for item in core_entities],
        *[make_new_company_state(item, Bucket.SECONDARY_CANDIDATES, CurrentRoute.INCUMBENT_REVIEW) for item in secondary_entities],
        *[make_new_company_state(item, Bucket.HIGH_BETA_WATCH, CurrentRoute.INCUMBENT_REVIEW) for item in high_beta_entities],
    ]
    source_kind = str(config.get("source_kind", "generated")).lower()
    universe = UniverseState(
        universe_id=universe_id or f"{normalized_market}_macro_ai_live",
        market=normalized_market.upper(),
        effective_date=date.today(),
        eligible_entities=entities,
        entity_mapping_version="entity_map_generated_v3",
        universe_rules_version=f"universe_rules_{source_kind}_v3",
    )
    pool = ResearchPoolState(
        pool_id=pool_id or f"{normalized_market}_macro_ai_live_pool",
        market=normalized_market.upper(),
        current_pool_members=[item.entity_id for item in core_entities + secondary_entities + high_beta_entities],
        shadow_watch_members=[],
        archived_members=[],
    )
    return universe, pool, companies
