from __future__ import annotations

import importlib
import json
import os
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import median
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import (
    AdapterPayload,
    DataProviderConfig,
    DataSourceHealthRecord,
    DataSourceManifestItem,
    EntityProfile,
    EventType,
    SourceType,
    TriggerEventRecord,
)
from .runtime_env import read_env_value
from .utils import clamp, make_id


def _events_count(trigger_events: list[TriggerEventRecord], event_type: EventType) -> int:
    return sum(1 for event in trigger_events if event.event_type == event_type)


def _safe_number(value: Any) -> float | None:
    if value in (None, "", "-", "--", "nan", "NaN"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    number = _safe_number(value)
    if number is None:
        return None
    return int(number)


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _hours_since(value: Any, fallback_hours: int = 48) -> int:
    observed = _coerce_date(value)
    if observed is None:
        return fallback_hours
    delta = date.today() - observed
    return max(0, delta.days * 24)


def _records_from_frame(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, list):
        return [item for item in frame if isinstance(item, dict)]
    if isinstance(frame, dict):
        return [frame]
    if hasattr(frame, "to_dict"):
        try:
            records = frame.to_dict("records")
        except TypeError:
            records = frame.to_dict()
        if isinstance(records, list):
            return [item for item in records if isinstance(item, dict)]
    return []


def _get_record_value(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _read_token(config: DataProviderConfig) -> str | None:
    if config.auth_env_var:
        token = read_env_value(config.auth_env_var)
        if token:
            return token
    token = config.provider_options.get("token")
    if token:
        return str(token)
    return None


def _infer_market(entity: EntityProfile | None, config: DataProviderConfig) -> str:
    market_hint = str(config.provider_options.get("market", "")).lower()
    if market_hint in {"cn", "a", "ashare", "a_share"}:
        return "cn"
    if market_hint in {"hk", "h", "hongkong", "hong_kong"}:
        return "hk"
    ticker = entity.ticker if entity is not None else ""
    normalized = ticker.upper()
    if normalized.endswith(".HK"):
        return "hk"
    if normalized.endswith((".SH", ".SZ", ".BJ")):
        return "cn"
    digits = "".join(char for char in normalized if char.isdigit())
    if len(digits) == 5:
        return "hk"
    return "cn"


def _normalize_symbol(entity: EntityProfile, market: str) -> str:
    ticker = entity.ticker.upper()
    raw = ticker.split(".", 1)[0]
    digits = "".join(char for char in raw if char.isdigit())
    if market == "hk":
        return digits.zfill(5) if digits else raw
    return digits.zfill(6) if digits else raw


def _normalize_tushare_code(entity: EntityProfile, market: str) -> str:
    ticker = entity.ticker.upper()
    if "." in ticker:
        return ticker
    symbol = _normalize_symbol(entity, market)
    if market == "hk":
        return f"{symbol}.HK"
    if symbol.startswith(("6", "9")):
        suffix = "SH"
    elif symbol.startswith("8"):
        suffix = "BJ"
    else:
        suffix = "SZ"
    return f"{symbol}.{suffix}"


def _first_record(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    return records[0] if records else None


def _score_from_ranges(value: float | None, *, low_good: float, high_bad: float) -> float | None:
    if value is None or high_bad <= low_good:
        return None
    normalized = 1 - ((value - low_good) / (high_bad - low_good))
    return round(clamp(normalized * 100), 2)


def _score_tradability(turnover_rate: float | None, amount: float | None, volume_ratio: float | None) -> float | None:
    parts: list[float] = []
    if turnover_rate is not None:
        parts.append(clamp(turnover_rate * 10, 0, 100))
    if amount is not None:
        parts.append(clamp(amount / 1_000_000_000 * 15, 0, 100))
    if volume_ratio is not None:
        parts.append(clamp(volume_ratio * 35, 0, 100))
    if not parts:
        return None
    return round(sum(parts) / len(parts), 2)


def _derive_filing_metrics(record: dict[str, Any]) -> dict[str, Any]:
    pe_value = _safe_number(_get_record_value(record, "pe_ttm", "pe", "pe_dynamic", "市盈率-动态", "PE", "ttm_pe"))
    pb_value = _safe_number(_get_record_value(record, "pb", "pb_ratio", "市净率", "PB"))
    turnover_rate = _safe_number(
        _get_record_value(record, "turnover_rate", "turnover_rate_f", "换手率", "turnoverRatio")
    )
    amount = _safe_number(_get_record_value(record, "amount", "成交额", "成交金额", "turnover"))
    volume_ratio = _safe_number(_get_record_value(record, "volume_ratio", "量比", "volumeRatio"))
    roe = _safe_number(_get_record_value(record, "roe", "roe_dt", "净资产收益率", "ROE"))
    gross_margin = _safe_number(_get_record_value(record, "grossprofit_margin", "gross_margin"))
    score_candidates = [
        _score_from_ranges(pe_value, low_good=8, high_bad=80),
        _score_from_ranges(pb_value, low_good=1, high_bad=12),
    ]
    valuation_values = [item for item in score_candidates if item is not None]
    values: dict[str, Any] = {}
    if valuation_values:
        values["valuation_score"] = round(sum(valuation_values) / len(valuation_values), 2)
    tradability = _score_tradability(turnover_rate, amount, volume_ratio)
    if tradability is not None:
        values["composite_tradability"] = tradability
    if roe is not None:
        values["base_quality"] = round(clamp(roe * 4.5, 0, 100), 2)
    if gross_margin is not None:
        values["industry_position_score"] = round(clamp(gross_margin * 1.8, 0, 100), 2)
    as_of_value = _get_record_value(record, "trade_date", "end_date", "date", "tradeDate")
    values["data_freshness_hours"] = _hours_since(as_of_value)
    return values


def _derive_quote_metrics(record: dict[str, Any]) -> dict[str, Any]:
    pct_chg = _safe_number(_get_record_value(record, "pct_chg", "change", "changeRatio", "涨跌幅"))
    amount = _safe_number(_get_record_value(record, "amount", "成交额", "turnover", "turnoverValue"))
    volume = _safe_number(_get_record_value(record, "vol", "volume"))
    values: dict[str, Any] = {}
    if pct_chg is not None:
        values["catalyst_score"] = round(clamp(55 + abs(pct_chg) * 6, 0, 100), 2)
    if amount is not None or volume is not None:
        tradability = clamp((amount or 0) / 100_000_000 * 3 + (volume or 0) / 1_000_000 * 2, 0, 100)
        values["composite_tradability"] = round(tradability, 2)
    as_of_value = _get_record_value(record, "trade_date", "date")
    values["data_freshness_hours"] = _hours_since(as_of_value)
    return values


def _derive_market_breadth(records: list[dict[str, Any]], provider_id: str) -> AdapterPayload | None:
    if not records:
        return None
    positive_moves = 0
    observed_moves = 0
    active_turnover = 0
    observed_turnover = 0
    change_values: list[float] = []
    turnover_values: list[float] = []
    for record in records:
        change_pct = _safe_number(_get_record_value(record, "涨跌幅", "pct_chg", "change_percent", "changePercent"))
        if change_pct is not None:
            observed_moves += 1
            change_values.append(change_pct)
            if change_pct > 0:
                positive_moves += 1
        turnover = _safe_number(_get_record_value(record, "成交额", "amount", "turnover", "成交金额"))
        if turnover is not None:
            observed_turnover += 1
            turnover_values.append(turnover)
            if turnover > 0:
                active_turnover += 1
    if observed_moves == 0:
        return None
    losers_count = max(0, observed_moves - positive_moves)
    advance_ratio = positive_moves / observed_moves
    turnover_ratio = (active_turnover / observed_turnover) if observed_turnover else 0.85
    median_change_pct = round(median(change_values), 3) if change_values else 0.0
    breadth_thrust = round(clamp(advance_ratio * 70 + max(-5.0, min(5.0, median_change_pct)) * 6, 0, 100), 2)
    liquidity_breadth = round(clamp(advance_ratio * 45 + turnover_ratio * 25 + breadth_thrust * 0.30, 0, 100), 2)
    turnover_median = round(median(turnover_values), 2) if turnover_values else 0.0
    health = DataSourceHealthRecord(
        record_id=make_id("health"),
        data_domain="market_breadth",
        primary_source=provider_id,
        backup_source=None,
        field_completeness=1.0,
        latency_hours=2.0,
        status="healthy",
        degradation_path="vendor_market_breadth",
    )
    return AdapterPayload(
        data_domain="market_breadth",
        values={
            "liquidity_breadth": liquidity_breadth,
            "advance_ratio": round(advance_ratio, 4),
            "active_turnover_ratio": round(turnover_ratio, 4),
            "median_change_pct": median_change_pct,
            "breadth_thrust": breadth_thrust,
            "gainers_count": positive_moves,
            "losers_count": losers_count,
            "sample_size": observed_moves,
            "turnover_median": turnover_median,
        },
        source_manifest=[
            DataSourceManifestItem(
                source_name=provider_id,
                source_type=SourceType.SYSTEM,
                as_of=datetime.now(UTC),
                field_coverage=1.0,
            )
        ],
        health_record=health,
        degraded=False,
        missing_reason=None,
    )


def _pick_symbol_record(records: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    symbol_candidates = {symbol.upper(), symbol.zfill(5), symbol.zfill(6)}
    for record in records:
        record_symbol = _get_record_value(
            record,
            "代码",
            "symbol",
            "ts_code",
            "代码简称",
            "证券代码",
            "stockCode",
            "code",
        )
        if record_symbol is None:
            continue
        candidate = str(record_symbol).upper().split(".", 1)[0]
        if candidate in symbol_candidates:
            return record
    return None


def _akshare_function_candidates(config: DataProviderConfig, market: str) -> list[str]:
    configured = str(
        config.provider_options.get(
            "function",
            "stock_hk_spot" if market == "hk" else "stock_zh_a_spot",
        )
    )
    defaults = ["stock_hk_spot", "stock_hk_spot_em"] if market == "hk" else ["stock_zh_a_spot", "stock_zh_a_spot_em"]
    ordered = [configured, *defaults]
    seen: set[str] = set()
    return [name for name in ordered if not (name in seen or seen.add(name))]


def _extract_json_path(payload: Any, path: list[Any]) -> Any:
    current = payload
    for part in path:
        if isinstance(part, int):
            if not isinstance(current, list) or part >= len(current):
                return None
            current = current[part]
        elif isinstance(current, dict):
            current = current.get(str(part))
        else:
            return None
        if current is None:
            return None
    return current


class BaseProvider(ABC):
    def __init__(self, config: DataProviderConfig):
        self.config = config

    def _health(self, status: str, degradation_path: str, backup_source: str | None = None) -> DataSourceHealthRecord:
        return DataSourceHealthRecord(
            record_id=make_id("health"),
            data_domain=self.config.data_domain,
            primary_source=self.config.provider_id,
            backup_source=backup_source,
            field_completeness=self.config.field_completeness,
            latency_hours=self.config.latency_hours,
            status=status,
            degradation_path=degradation_path,
        )

    def _manifest(self, source_type: SourceType) -> list[DataSourceManifestItem]:
        return [
            DataSourceManifestItem(
                source_name=self.config.provider_id,
                source_type=source_type,
                as_of=datetime.now(UTC),
                field_coverage=self.config.field_completeness,
            )
        ]

    def _degraded_payload(
        self,
        reason: str,
        *,
        source_type: SourceType = SourceType.SYSTEM,
        values: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        return AdapterPayload(
            data_domain=self.config.data_domain,
            values=values or {},
            source_manifest=self._manifest(source_type),
            health_record=self._health("degraded", reason),
            degraded=True,
            missing_reason=reason,
        )

    @abstractmethod
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        raise NotImplementedError


class MockProvider(BaseProvider):
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        trigger_events = trigger_events or []
        values = dict(self.config.static_values)
        degraded = False

        if self.config.data_domain == "macro_prices":
            macro_count = _events_count(trigger_events, EventType.MACRO)
            values.setdefault("ust_10y", round(4.1 + macro_count * 0.15, 2))
            values.setdefault(
                "volatility_regime",
                round(18 + macro_count * 2.5 + _events_count(trigger_events, EventType.NEWS) * 1.5, 2),
            )
            degraded = macro_count >= 2 or self.config.field_completeness < 0.85
            source_type = SourceType.PIPELINE
        elif self.config.data_domain == "market_breadth":
            macro_count = _events_count(trigger_events, EventType.MACRO)
            liquidity_breadth = round(61 - macro_count * 4 - _events_count(trigger_events, EventType.NEWS) * 2, 2)
            values.setdefault(
                "liquidity_breadth",
                liquidity_breadth,
            )
            values.setdefault("advance_ratio", round(clamp((liquidity_breadth - 18) / 82, 0, 1), 4))
            values.setdefault("active_turnover_ratio", round(clamp((liquidity_breadth + 10) / 100, 0, 1), 4))
            values.setdefault("median_change_pct", round((liquidity_breadth - 50) / 12, 3))
            values.setdefault("breadth_thrust", round(clamp(liquidity_breadth * 0.92, 0, 100), 2))
            values.setdefault("gainers_count", int(2400 + max(-600, (liquidity_breadth - 50) * 45)))
            values.setdefault("losers_count", int(2800 - max(-600, (liquidity_breadth - 50) * 45)))
            values.setdefault("sample_size", values["gainers_count"] + values["losers_count"])
            values.setdefault("turnover_median", round(850_000_000 + liquidity_breadth * 8_500_000, 2))
            degraded = self.config.field_completeness < 0.85
            source_type = SourceType.PIPELINE
        elif self.config.data_domain == "sector_signals":
            macro_count = _events_count(trigger_events, EventType.MACRO)
            earnings_boost = _events_count(trigger_events, EventType.EARNINGS) + _events_count(
                trigger_events, EventType.GUIDANCE
            )
            values.setdefault("ai_capex_momentum", round(72 + earnings_boost * 4 - macro_count * 3, 2))
            degraded = macro_count >= 2 or self.config.field_completeness < 0.88
            source_type = SourceType.PIPELINE
        elif self.config.data_domain == "filing_metrics" and entity is not None:
            values.setdefault("base_quality", entity.base_quality)
            values.setdefault("industry_position_score", entity.industry_position_score)
            values.setdefault("macro_alignment_score", entity.macro_alignment_score)
            values.setdefault("valuation_score", entity.valuation_score)
            values.setdefault("catalyst_score", entity.catalyst_score)
            values.setdefault("risk_penalty", entity.risk_penalty)
            values.setdefault("composite_tradability", round((entity.liquidity_score + entity.tradability_score) / 2, 2))
            values.setdefault("data_freshness_hours", min(entity.evidence_freshness_days * 12, 240))
            degraded = entity.info_score < 75 or self.config.field_completeness < 0.82
            source_type = SourceType.FILING
        else:
            source_type = SourceType.SYSTEM

        status = "degraded" if degraded else "healthy"
        return AdapterPayload(
            data_domain=self.config.data_domain,
            values=values,
            source_manifest=self._manifest(source_type),
            health_record=self._health(status, f"{self.config.data_domain}_fallback"),
            degraded=degraded,
            missing_reason=None,
        )


class FileSnapshotProvider(BaseProvider):
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        if not self.config.file_path:
            return self._degraded_payload("missing_file_path")
        path = Path(self.config.file_path)
        if not path.exists():
            return self._degraded_payload("file_not_found")
        payload = json.loads(path.read_text(encoding="utf-8"))
        values = payload.get(self.config.data_domain, payload)
        if entity is not None and isinstance(values, dict) and entity.entity_id in values:
            values = values[entity.entity_id]
        elif entity is not None and isinstance(values, dict):
            return self._degraded_payload("entity_not_found_in_snapshot")
        return AdapterPayload(
            data_domain=self.config.data_domain,
            values=values,
            source_manifest=self._manifest(SourceType.SYSTEM),
            health_record=self._health(
                "degraded" if self.config.field_completeness < 0.85 else "healthy",
                "file_snapshot_fallback",
            ),
            degraded=self.config.field_completeness < 0.85,
            missing_reason=None,
        )


class HttpJsonProvider(BaseProvider):
    def _request_json(self, query: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
        if not self.config.base_url:
            raise ValueError("missing_base_url")
        url = self.config.base_url.rstrip("/")
        if self.config.endpoint:
            url = f"{url}/{self.config.endpoint.lstrip('/')}"
        if query:
            url = f"{url}?{urlencode(query)}"
        headers = {"Accept": "application/json"}
        token = _read_token(self.config)
        if token:
            auth_mode = str(self.config.provider_options.get("auth_mode", "bearer")).lower()
            if auth_mode == "header":
                header_name = str(self.config.provider_options.get("auth_header", "X-API-KEY"))
                headers[header_name] = token
            else:
                headers["Authorization"] = f"Bearer {token}"
        request = Request(url, headers=headers, method="GET")
        with urlopen(request, timeout=self.config.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        query = dict(params or {})
        if entity is not None:
            query.setdefault("entity_id", entity.entity_id)
            query.setdefault("ticker", entity.ticker)
        if trigger_events:
            query.setdefault("event_count", str(len(trigger_events)))
        try:
            payload = self._request_json(query)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            return self._degraded_payload(str(exc))
        values = payload.get("values", payload) if isinstance(payload, dict) else {}
        status = "degraded" if self.config.field_completeness < 0.85 else "healthy"
        return AdapterPayload(
            data_domain=self.config.data_domain,
            values=values,
            source_manifest=self._manifest(SourceType.SYSTEM),
            health_record=self._health(status, "http_provider_fallback"),
            degraded=status == "degraded",
            missing_reason=None,
        )


class TushareProvider(BaseProvider):
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        try:
            tushare = importlib.import_module("tushare")
        except ImportError:
            return self._degraded_payload("tushare_not_installed")
        token = _read_token(self.config)
        if not token:
            return self._degraded_payload("missing_tushare_token")
        client = tushare.pro_api(token)
        try:
            if self.config.data_domain == "filing_metrics":
                return self._fetch_filing_metrics(client, entity)
            if self.config.data_domain == "market_breadth":
                return self._fetch_market_breadth(client)
        except Exception as exc:  # pragma: no cover - vendor SDK specifics
            return self._degraded_payload(f"tushare_error:{exc}")
        return self._degraded_payload("unsupported_tushare_domain")

    def _fetch_filing_metrics(self, client: Any, entity: EntityProfile | None) -> AdapterPayload:
        if entity is None:
            return self._degraded_payload("missing_entity", source_type=SourceType.FILING)
        market = _infer_market(entity, self.config)
        ts_code = _normalize_tushare_code(entity, market)
        api_candidates = ["hk_daily"] if market == "hk" else ["daily_basic", "daily"]
        last_error: str | None = None
        for api_name in api_candidates:
            if not hasattr(client, api_name):
                last_error = "unsupported_tushare_api"
                continue
            try:
                frame = getattr(client, api_name)(ts_code=ts_code, limit=1)
            except Exception as exc:  # pragma: no cover - vendor SDK specifics
                last_error = str(exc)
                continue
            record = _first_record(_records_from_frame(frame))
            if record is None:
                last_error = "empty_tushare_response"
                continue
            values = _derive_filing_metrics(record)
            values.update({key: value for key, value in _derive_quote_metrics(record).items() if key not in values})
            if not values:
                last_error = "empty_tushare_fields"
                continue
            health_status = "healthy" if api_name == "daily_basic" else "partial"
            return AdapterPayload(
                data_domain="filing_metrics",
                values=values,
                source_manifest=self._manifest(SourceType.FILING),
                health_record=self._health(health_status, f"tushare_{api_name}"),
                degraded=False,
                missing_reason=None,
            )
        return self._degraded_payload(last_error or "unsupported_tushare_api", source_type=SourceType.FILING)

    def _fetch_market_breadth(self, client: Any) -> AdapterPayload:
        api_name = str(self.config.provider_options.get("breadth_api", "daily_info"))
        if api_name == "daily_proxy":
            return self._fetch_market_breadth_from_daily(client)
        if not hasattr(client, api_name):
            return self._degraded_payload("unsupported_tushare_api")
        exchange = self.config.provider_options.get("exchange", "SSE")
        frame = getattr(client, api_name)(exchange=exchange)
        records = _records_from_frame(frame)
        if not records:
            return self._fetch_market_breadth_from_daily(client)
        record = records[0]
        amount = _safe_number(_get_record_value(record, "amount", "成交额", "成交金额"))
        turnover = _safe_number(_get_record_value(record, "vol", "volume"))
        liquidity = round(clamp((amount or 0) / 1_000_000_000 * 8 + (100 if (turnover or 0) > 0 else 45), 0, 100), 2)
        return AdapterPayload(
            data_domain="market_breadth",
            values={"liquidity_breadth": liquidity},
            source_manifest=self._manifest(SourceType.SYSTEM),
            health_record=self._health("healthy", "tushare_market_breadth"),
            degraded=False,
            missing_reason=None,
        )

    def _fetch_market_breadth_from_daily(self, client: Any) -> AdapterPayload:
        if not hasattr(client, "daily"):
            return self._degraded_payload("unsupported_tushare_api")
        latest_frame = client.daily(ts_code="688981.SH", limit=1)
        latest_record = _first_record(_records_from_frame(latest_frame))
        if latest_record is None:
            return self._degraded_payload("empty_tushare_response")
        trade_date = _get_record_value(latest_record, "trade_date")
        if not trade_date:
            return self._degraded_payload("empty_tushare_response")
        frame = client.daily(trade_date=str(trade_date))
        records = _records_from_frame(frame)
        payload = _derive_market_breadth(records, self.config.provider_id)
        if payload is None:
            return self._degraded_payload("empty_tushare_response")
        return payload.model_copy(
            update={
                "health_record": self._health("healthy", "tushare_market_breadth:daily_proxy"),
                "source_manifest": self._manifest(SourceType.SYSTEM),
            }
        )


class AkshareProvider(BaseProvider):
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        try:
            akshare = importlib.import_module("akshare")
        except ImportError:
            return self._degraded_payload("akshare_not_installed")
        try:
            if self.config.data_domain == "market_breadth":
                return self._fetch_market_breadth(akshare)
            if self.config.data_domain == "filing_metrics":
                return self._fetch_filing_metrics(akshare, entity)
        except Exception as exc:  # pragma: no cover - vendor SDK specifics
            return self._degraded_payload(f"akshare_error:{exc}")
        return self._degraded_payload("unsupported_akshare_domain")

    def _fetch_market_breadth(self, akshare: Any) -> AdapterPayload:
        market = str(self.config.provider_options.get("market", "cn")).lower()
        call_kwargs = dict(self.config.provider_options.get("call_kwargs", {}))
        last_error = "unsupported_akshare_function"
        for function_name in _akshare_function_candidates(self.config, market):
            if not hasattr(akshare, function_name):
                continue
            try:
                frame = getattr(akshare, function_name)(**call_kwargs)
            except Exception as exc:
                last_error = f"akshare_error:{exc}"
                continue
            records = _records_from_frame(frame)
            payload = _derive_market_breadth(records, self.config.provider_id)
            if payload is None:
                last_error = "empty_akshare_response"
                continue
            return payload.model_copy(
                update={
                    "health_record": self._health("healthy", f"akshare_market_breadth:{function_name}"),
                    "source_manifest": self._manifest(SourceType.SYSTEM),
                }
            )
        return self._degraded_payload(last_error)

    def _fetch_filing_metrics(self, akshare: Any, entity: EntityProfile | None) -> AdapterPayload:
        if entity is None:
            return self._degraded_payload("missing_entity", source_type=SourceType.FILING)
        market = _infer_market(entity, self.config)
        call_kwargs = dict(self.config.provider_options.get("call_kwargs", {}))
        symbol = _normalize_symbol(entity, market)
        last_error = "unsupported_akshare_function"
        for function_name in _akshare_function_candidates(self.config, market):
            if not hasattr(akshare, function_name):
                continue
            try:
                frame = getattr(akshare, function_name)(**call_kwargs)
            except Exception as exc:
                last_error = f"akshare_error:{exc}"
                continue
            records = _records_from_frame(frame)
            record = _pick_symbol_record(records, symbol)
            if record is None:
                last_error = "symbol_not_found_in_akshare"
                continue
            values = _derive_filing_metrics(record)
            if "data_freshness_hours" not in values:
                values["data_freshness_hours"] = 24
            if not values:
                last_error = "empty_akshare_fields"
                continue
            return AdapterPayload(
                data_domain="filing_metrics",
                values=values,
                source_manifest=self._manifest(SourceType.FILING),
                health_record=self._health("healthy", f"akshare_filing:{function_name}"),
                degraded=False,
                missing_reason=None,
            )
        return self._degraded_payload(last_error, source_type=SourceType.FILING)


class AllTickProvider(BaseProvider):
    def fetch(
        self,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict[str, Any] | None = None,
    ) -> AdapterPayload:
        token = _read_token(self.config)
        if not token:
            return self._degraded_payload("missing_alltick_token")
        if not self.config.base_url:
            return self._degraded_payload("missing_base_url")
        try:
            if self.config.data_domain == "filing_metrics":
                return self._fetch_filing_metrics(entity, token)
            if self.config.data_domain == "market_breadth":
                return self._fetch_market_breadth(token)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            return self._degraded_payload(f"alltick_error:{exc}")
        return self._degraded_payload("unsupported_alltick_domain")

    def _request_json(self, token: str, query: dict[str, Any]) -> dict[str, Any] | list[Any]:
        base_url = self.config.base_url.rstrip("/")
        endpoint = self.config.endpoint or str(self.config.provider_options.get("endpoint", ""))
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
        if query:
            url = f"{url}?{urlencode(query, doseq=True)}"
        header_name = str(self.config.provider_options.get("auth_header", "token"))
        headers = {"Accept": "application/json", header_name: token}
        request = Request(url, headers=headers, method="GET")
        with urlopen(request, timeout=self.config.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _payload_records(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        response_path = self.config.provider_options.get("response_path", ["data"])
        if not isinstance(response_path, list):
            response_path = [response_path]
        data = _extract_json_path(payload, response_path) if response_path else payload
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def _build_symbol(self, entity: EntityProfile) -> str:
        market = _infer_market(entity, self.config)
        symbol = _normalize_symbol(entity, market)
        template = str(self.config.provider_options.get("symbol_template", "{ticker}"))
        return template.format(
            ticker=symbol,
            raw_ticker=entity.ticker,
            market=market.upper(),
        )

    def _fetch_filing_metrics(self, entity: EntityProfile | None, token: str) -> AdapterPayload:
        if entity is None:
            return self._degraded_payload("missing_entity", source_type=SourceType.FILING)
        symbol_param = str(self.config.provider_options.get("symbol_param", "symbol"))
        query = {symbol_param: self._build_symbol(entity)}
        query.update(self.config.provider_options.get("query_params", {}))
        payload = self._request_json(token, query)
        record = _first_record(self._payload_records(payload))
        if record is None:
            return self._degraded_payload("empty_alltick_response", source_type=SourceType.FILING)
        values = _derive_filing_metrics(record)
        turnover = _safe_number(_get_record_value(record, "turnover", "成交额", "amount", "turnoverValue"))
        latest_change = _safe_number(_get_record_value(record, "changeRatio", "涨跌幅", "pct_chg"))
        if "composite_tradability" not in values and turnover is not None:
            values["composite_tradability"] = round(clamp(turnover / 100_000_000 * 20, 0, 100), 2)
        if latest_change is not None and "catalyst_score" not in values:
            values["catalyst_score"] = round(clamp(abs(latest_change) * 10, 0, 100), 2)
        if "data_freshness_hours" not in values:
            values["data_freshness_hours"] = 1
        if not values:
            return self._degraded_payload("empty_alltick_fields", source_type=SourceType.FILING)
        return AdapterPayload(
            data_domain="filing_metrics",
            values=values,
            source_manifest=self._manifest(SourceType.FILING),
            health_record=self._health("healthy", "alltick_quote"),
            degraded=False,
            missing_reason=None,
        )

    def _fetch_market_breadth(self, token: str) -> AdapterPayload:
        symbols = list(self.config.provider_options.get("symbols", []))
        if not symbols:
            return self._degraded_payload("missing_alltick_symbols")
        symbol_param = str(self.config.provider_options.get("symbol_param", "symbol"))
        query = {symbol_param: symbols}
        query.update(self.config.provider_options.get("query_params", {}))
        payload = self._request_json(token, query)
        records = self._payload_records(payload)
        breadth_payload = _derive_market_breadth(records, self.config.provider_id)
        if breadth_payload is None:
            return self._degraded_payload("empty_alltick_response")
        return breadth_payload.model_copy(
            update={
                "health_record": self._health("healthy", "alltick_market_breadth"),
                "source_manifest": self._manifest(SourceType.SYSTEM),
            }
        )


def make_provider(config: DataProviderConfig) -> BaseProvider:
    if config.provider_kind == "file":
        return FileSnapshotProvider(config)
    if config.provider_kind == "http":
        return HttpJsonProvider(config)
    if config.provider_kind == "tushare":
        return TushareProvider(config)
    if config.provider_kind == "akshare":
        return AkshareProvider(config)
    if config.provider_kind == "alltick":
        return AllTickProvider(config)
    return MockProvider(config)
