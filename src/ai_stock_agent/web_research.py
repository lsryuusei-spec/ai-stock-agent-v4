from __future__ import annotations

import importlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import (
    EntityProfile,
    EventType,
    SourceCollectionDiagnostic,
    SourceType,
    TriggerEventRecord,
    WakeScope,
    WebEvidenceItem,
    WebResearchProviderConfig,
    WebResearchRecord,
)
from .official_sources import OfficialSourceHarvester
from .runtime_env import read_env_value
from .utils import make_id, model_hash


DEFAULT_WEB_PROVIDER_CONFIGS = [
    WebResearchProviderConfig(
        provider_id="web_mock",
        provider_kind="mock",
        enabled=True,
        max_results=3,
        static_keywords=["earnings", "guidance", "capacity", "pricing", "AI demand"],
    )
]

TRIGGER_KEYWORDS: dict[EventType, tuple[str, ...]] = {
    EventType.EARNINGS: ("earnings", "results", "profit", "revenue", "\u8d22\u62a5", "\u4e1a\u7ee9"),
    EventType.GUIDANCE: ("guidance", "outlook", "forecast", "\u6307\u5f15", "\u5c55\u671b"),
    EventType.PRICE_BREAK: ("surge", "breakout", "hot rank", "limit up", "\u5f02\u52a8", "\u70ed\u5ea6"),
    EventType.MACRO: ("macro", "policy", "rates", "liquidity", "\u653f\u7b56", "\u6d41\u52a8\u6027"),
}

SOURCE_TIER_RULES: list[tuple[str, float, tuple[str, ...]]] = [
    (
        "official",
        0.95,
        (
            "cninfo.com.cn",
            "hkex.com.hk",
            "sec.gov",
            "investor",
            "ir.",
            "announcement",
            "disclosure",
            "\u516c\u544a",
            "\u62ab\u9732",
            "\u4ea4\u6613\u6240",
        ),
    ),
    (
        "tier1_media",
        0.86,
        ("reuters", "bloomberg", "wsj", "ft.com", "caixin", "\u8d22\u65b0", "yicai", "\u7b2c\u4e00\u8d22\u7ecf"),
    ),
    (
        "market_data",
        0.76,
        ("eastmoney", "sina", "\u96ea\u7403", "xueqiu", "akshare", "cninfo", "\u4e92\u52a8\u6613", "irm"),
    ),
]

COOLDOWN_WINDOWS: dict[EventType, int] = {
    EventType.EARNINGS: 72,
    EventType.GUIDANCE: 48,
    EventType.PRICE_BREAK: 12,
    EventType.MACRO: 24,
    EventType.NEWS: 18,
    EventType.MANUAL: 0,
}


def load_web_provider_configs(config_path: str | None = None) -> list[WebResearchProviderConfig]:
    if not config_path:
        return list(DEFAULT_WEB_PROVIDER_CONFIGS)
    path = Path(config_path)
    if not path.exists():
        return list(DEFAULT_WEB_PROVIDER_CONFIGS)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [WebResearchProviderConfig.model_validate(item) for item in payload]


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


def _pick_value(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if value in (None, "", "-", "--"):
        return datetime.now(UTC)
    text = str(value).strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return datetime.now(UTC)


def _normalize_market(entity: EntityProfile | None) -> str:
    if entity is None:
        return "macro"
    ticker = entity.ticker.upper()
    if ticker.endswith(".HK"):
        return "hk"
    if ticker.endswith((".SZ", ".SH", ".BJ")):
        return "cn"
    digits = "".join(char for char in ticker if char.isdigit())
    if len(digits) == 5:
        return "hk"
    return "cn"


def _symbol(entity: EntityProfile) -> str:
    digits = "".join(char for char in entity.ticker if char.isdigit())
    market = _normalize_market(entity)
    if market == "hk":
        return digits.zfill(5)
    return digits.zfill(6) if digits else entity.ticker.split(".", 1)[0]


def _render_snippet(record: dict[str, Any]) -> str:
    parts = [
        _pick_value(record, "\u6458\u8981", "snippet", "content", "\u5185\u5bb9", "\u5177\u4f53\u4e8b\u9879", "question", "\u95ee\u9898"),
        _pick_value(record, "answer", "\u56de\u7b54", "latest_price", "\u6700\u65b0\u4ef7"),
    ]
    text = " ".join(str(item).strip() for item in parts if item not in (None, ""))
    return text[:220] if text else "AKShare evidence item"


def _normalize_text(value: str) -> str:
    return " ".join(str(value).lower().replace("|", " ").split())


def _infer_source_quality(
    *,
    provider_id: str,
    source_name: str,
    url: str,
    evidence_type: str,
) -> tuple[str, float]:
    lowered = _normalize_text(f"{provider_id} {source_name} {url} {evidence_type}")
    for tier, score, keywords in SOURCE_TIER_RULES:
        if any(keyword in lowered for keyword in keywords):
            return tier, score
    if "mock" in lowered:
        return "synthetic", 0.45
    if "file" in lowered:
        return "file_snapshot", 0.66
    return "general_web", 0.62


def _content_hash_for_item(
    *,
    title: str,
    snippet: str,
    url: str,
    source_name: str,
) -> str:
    return model_hash(
        {
            "title": _normalize_text(title),
            "snippet": _normalize_text(snippet)[:160],
            "url": url.split("?", 1)[0],
            "source_name": _normalize_text(source_name),
        }
    )


def _annotate_evidence_item(item: WebEvidenceItem, provider_id: str) -> WebEvidenceItem:
    source_tier, source_score = _infer_source_quality(
        provider_id=provider_id,
        source_name=item.source_name,
        url=item.url,
        evidence_type=item.evidence_type,
    )
    return item.model_copy(
        update={
            "source_tier": source_tier,
            "source_score": source_score,
            "content_hash": _content_hash_for_item(
                title=item.title,
                snippet=item.snippet,
                url=item.url,
                source_name=item.source_name,
            ),
        }
    )


def _record_to_evidence(
    record: dict[str, Any],
    *,
    entity: EntityProfile | None,
    provider_id: str,
    evidence_type: str,
) -> WebEvidenceItem | None:
    title = _pick_value(record, "title", "\u6807\u9898", "\u65b0\u95fb\u6807\u9898", "name", "\u95ee\u9898", "question", "\u8bc1\u5238\u7b80\u79f0")
    if title is None:
        return None
    item = WebEvidenceItem(
        title=str(title),
        snippet=_render_snippet(record),
        url=str(_pick_value(record, "url", "\u65b0\u95fb\u94fe\u63a5", "\u94fe\u63a5", "article_url", "\u8d44\u6599\u5730\u5740", "\u7f51\u9875\u94fe\u63a5") or "https://example.com/akshare"),
        source_name=str(_pick_value(record, "\u6587\u7ae0\u6765\u6e90", "\u5a92\u4f53\u540d\u79f0", "source_name", "source") or provider_id),
        published_at=_to_datetime(
            _pick_value(record, "\u53d1\u5e03\u65f6\u95f4", "\u53d1\u5e03\u65f6\u95f4\u6233", "date", "trade_date", "\u516c\u544a\u65e5\u671f", "\u65f6\u95f4")
        ),
        entity_id=entity.entity_id if entity is not None else None,
        relevance_score=float(_pick_value(record, "relevance_score") or 0.78),
        evidence_type=evidence_type,
    )
    return _annotate_evidence_item(item, provider_id)


class BaseWebProvider:
    def __init__(self, config: WebResearchProviderConfig):
        self.config = config
        self.last_diagnostics: list[SourceCollectionDiagnostic] = []

    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        raise NotImplementedError


class MockWebProvider(BaseWebProvider):
    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        label = entity.company_name if entity is not None else "Macro theme"
        items: list[WebEvidenceItem] = []
        keywords = self.config.static_keywords or ["industry update", "guidance", "AI demand"]
        for index, keyword in enumerate(keywords[: self.config.max_results]):
            items.append(
                _annotate_evidence_item(
                    WebEvidenceItem(
                        title=f"{label}: {keyword}",
                        snippet=f"Synthesized web evidence for '{query}' highlighting {keyword}.",
                        url=f"https://example.com/research/{label.replace(' ', '-').lower()}/{index}",
                        source_name=self.config.provider_id,
                        published_at=datetime.now(UTC),
                        entity_id=entity.entity_id if entity is not None else None,
                        relevance_score=max(0.5, 0.9 - index * 0.1),
                        evidence_type="web_mock",
                    ),
                    self.config.provider_id,
                )
            )
        return items


class FileWebProvider(BaseWebProvider):
    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        if not self.config.file_path:
            return []
        path = Path(self.config.file_path)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if entity is not None:
            items = payload.get("entities", {}).get(entity.entity_id, [])
        else:
            items = payload.get("macro_theme", [])
        return [
            _annotate_evidence_item(
                WebEvidenceItem(
                    title=item["title"],
                    snippet=item["snippet"],
                    url=item["url"],
                    source_name=item.get("source_name", self.config.provider_id),
                    published_at=datetime.fromisoformat(item["published_at"].replace("Z", "+00:00")),
                    entity_id=entity.entity_id if entity is not None else None,
                    relevance_score=item.get("relevance_score", 0.7),
                    evidence_type=item.get("evidence_type", "web_file"),
                ),
                self.config.provider_id,
            )
            for item in items[: self.config.max_results]
        ]


class HttpWebProvider(BaseWebProvider):
    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        if not self.config.base_url:
            return []
        params = {"q": query, "max_results": str(self.config.max_results)}
        if entity is not None:
            params["ticker"] = entity.ticker
        url = self.config.base_url.rstrip("/")
        if self.config.endpoint:
            url = f"{url}/{self.config.endpoint.lstrip('/')}"
        url = f"{url}?{urlencode(params)}"
        headers = {"Accept": "application/json"}
        if self.config.auth_env_var and read_env_value(self.config.auth_env_var):
            headers["Authorization"] = f"Bearer {read_env_value(self.config.auth_env_var)}"
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            return []
        return [
            _annotate_evidence_item(
                WebEvidenceItem(
                    title=item["title"],
                    snippet=item["snippet"],
                    url=item["url"],
                    source_name=item.get("source_name", self.config.provider_id),
                    published_at=datetime.fromisoformat(item["published_at"].replace("Z", "+00:00")),
                    entity_id=entity.entity_id if entity is not None else None,
                    relevance_score=item.get("relevance_score", 0.7),
                    evidence_type=item.get("evidence_type", "web_http"),
                ),
                self.config.provider_id,
            )
            for item in payload.get("results", [])[: self.config.max_results]
        ]


class AkshareWebProvider(BaseWebProvider):
    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        try:
            akshare = importlib.import_module("akshare")
        except ImportError:
            return []
        if entity is None:
            return self._macro_search(akshare)
        if _normalize_market(entity) == "hk":
            return self._hk_entity_search(akshare, entity)
        return self._cn_entity_search(akshare, entity)

    def _macro_search(self, akshare: Any) -> list[WebEvidenceItem]:
        results: list[WebEvidenceItem] = []
        if hasattr(akshare, "news_report_time_baidu"):
            try:
                records = _records_from_frame(akshare.news_report_time_baidu(date=date.today().strftime("%Y%m%d")))
            except Exception:
                records = []
            for record in records[: self.config.max_results]:
                evidence = _record_to_evidence(
                    record,
                    entity=None,
                    provider_id=self.config.provider_id,
                    evidence_type="akshare_macro_news",
                )
                if evidence is not None:
                    results.append(evidence)
        return results[: self.config.max_results]

    def _cn_entity_search(self, akshare: Any, entity: EntityProfile) -> list[WebEvidenceItem]:
        symbol = _symbol(entity)
        collected: list[WebEvidenceItem] = []
        sources = [
            ("stock_news_em", {"symbol": symbol}, "akshare_company_news"),
            ("stock_zh_a_disclosure_report_cninfo", {"symbol": symbol}, "akshare_notice"),
            ("stock_irm_cninfo", {"symbol": symbol}, "akshare_irm"),
        ]
        for function_name, kwargs, evidence_type in sources:
            if not hasattr(akshare, function_name):
                continue
            try:
                records = _records_from_frame(getattr(akshare, function_name)(**kwargs))
            except Exception:
                continue
            for record in records:
                evidence = _record_to_evidence(
                    record,
                    entity=entity,
                    provider_id=self.config.provider_id,
                    evidence_type=evidence_type,
                )
                if evidence is not None:
                    collected.append(evidence)
                if len(collected) >= self.config.max_results:
                    return self._dedupe(collected)
        return self._dedupe(collected)

    def _hk_entity_search(self, akshare: Any, entity: EntityProfile) -> list[WebEvidenceItem]:
        symbol = _symbol(entity)
        collected: list[WebEvidenceItem] = []
        sources = [
            ("stock_hk_hot_rank_latest_em", {"symbol": symbol}, "akshare_hk_hot_rank"),
            ("stock_hk_hot_rank_detail_realtime_em", {"symbol": symbol}, "akshare_hk_hot_rank_detail"),
        ]
        for function_name, kwargs, evidence_type in sources:
            if not hasattr(akshare, function_name):
                continue
            try:
                records = _records_from_frame(getattr(akshare, function_name)(**kwargs))
            except Exception:
                continue
            for record in records:
                evidence = _record_to_evidence(
                    record,
                    entity=entity,
                    provider_id=self.config.provider_id,
                    evidence_type=evidence_type,
                )
                if evidence is not None:
                    collected.append(evidence)
                if len(collected) >= self.config.max_results:
                    return self._dedupe(collected)
        return self._dedupe(collected)

    def _dedupe(self, items: list[WebEvidenceItem]) -> list[WebEvidenceItem]:
        deduped: list[WebEvidenceItem] = []
        seen: set[tuple[str, str]] = set()
        for item in items:
            key = (item.title, item.url)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= self.config.max_results:
                break
        return deduped


class OfficialWebProvider(BaseWebProvider):
    def __init__(self, config: WebResearchProviderConfig):
        super().__init__(config)
        self.harvester = OfficialSourceHarvester(config)

    def search(
        self,
        query: str,
        entity: EntityProfile | None = None,
        trigger_events: list[TriggerEventRecord] | None = None,
    ) -> list[WebEvidenceItem]:
        del trigger_events
        self.last_diagnostics = []
        items: list[WebEvidenceItem] = []
        for record in self.harvester.collect(query, entity):
            evidence_type = str(
                _pick_value(record, "evidence_type", "document_type", "category", "source_category")
                or "official_disclosure"
            )
            evidence = _record_to_evidence(
                record,
                entity=entity,
                provider_id=self.config.provider_id,
                evidence_type=evidence_type,
            )
            if evidence is None:
                continue
            items.append(evidence)
            if len(items) >= self.config.max_results:
                break
        self.last_diagnostics = list(self.harvester.last_diagnostics)
        return items


def _make_provider(config: WebResearchProviderConfig) -> BaseWebProvider:
    if config.provider_kind == "file":
        return FileWebProvider(config)
    if config.provider_kind == "http":
        return HttpWebProvider(config)
    if config.provider_kind == "official":
        return OfficialWebProvider(config)
    if config.provider_kind == "akshare":
        return AkshareWebProvider(config)
    return MockWebProvider(config)


class WebResearchManager:
    def __init__(self, configs: list[WebResearchProviderConfig]):
        self.configs = [config for config in configs if config.enabled]

    def collect(
        self,
        *,
        run_id: str,
        macro_theme: str,
        trigger_events: list[TriggerEventRecord],
        entities: list[EntityProfile],
    ) -> list[WebResearchRecord]:
        if not self.configs:
            return []
        records: list[WebResearchRecord] = []
        macro_query = f"{macro_theme} latest developments"
        for config in self.configs:
            provider = _make_provider(config)
            macro_items = provider.search(macro_query, None, trigger_events)
            records.append(
                WebResearchRecord(
                    record_id=make_id("web"),
                    run_id=run_id,
                    provider_id=config.provider_id,
                    macro_theme=macro_theme,
                    query=macro_query,
                    entity_id=None,
                    evidence_items=macro_items,
                    collection_diagnostics=list(getattr(provider, "last_diagnostics", [])),
                    summary=f"Collected {len(macro_items)} macro web evidence items.",
                    status="ok" if macro_items else "empty",
                )
            )
            for entity in entities:
                query = f"{entity.ticker} {macro_theme} latest news"
                evidence_items = provider.search(query, entity, trigger_events)
                records.append(
                    WebResearchRecord(
                        record_id=make_id("web"),
                        run_id=run_id,
                        provider_id=config.provider_id,
                        macro_theme=macro_theme,
                        query=query,
                        entity_id=entity.entity_id,
                        evidence_items=evidence_items,
                        collection_diagnostics=list(getattr(provider, "last_diagnostics", [])),
                        summary=f"Collected {len(evidence_items)} web evidence items for {entity.ticker}.",
                        status="ok" if evidence_items else "empty",
                    )
                )
        return records


def build_web_research_manager(config_path: str | None = None) -> WebResearchManager:
    return WebResearchManager(load_web_provider_configs(config_path))


def infer_event_type(item: WebEvidenceItem) -> EventType:
    lowered = f"{item.title} {item.snippet} {item.evidence_type}".lower()
    for event_type, keywords in TRIGGER_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return event_type
    return EventType.NEWS


def synthesize_trigger_events(
    *,
    run_id: str,
    records: list[WebResearchRecord],
    entity_map: dict[str, EntityProfile],
    existing_events: list[TriggerEventRecord] | None = None,
) -> list[TriggerEventRecord]:
    del run_id
    events: list[TriggerEventRecord] = []
    seen: set[str] = set()
    historical_events = list(existing_events or [])
    for record in records:
        if not record.evidence_items:
            continue
        top_item = max(
            record.evidence_items,
            key=lambda item: (
                item.relevance_score * 0.62
                + item.source_score * 0.28
                + (0.1 if item.source_tier == "official" else 0.04 if item.source_tier == "tier1_media" else 0.0)
            ),
        )
        event_type = infer_event_type(top_item)
        impacted = [record.entity_id] if record.entity_id and record.entity_id in entity_map else []
        evidence_signature = top_item.content_hash or _content_hash_for_item(
            title=top_item.title,
            snippet=top_item.snippet,
            url=top_item.url,
            source_name=top_item.source_name,
        )
        dedup_key = model_hash(
            {
                "provider_id": record.provider_id,
                "entity_id": record.entity_id,
                "content_hash": evidence_signature,
                "event_type": event_type.value,
            }
        )
        if dedup_key in seen:
            continue
        cooldown_group = f"web_{event_type.value}"
        cooldown_hours = COOLDOWN_WINDOWS.get(event_type, 24)
        cooldown_boundary = top_item.published_at - timedelta(hours=cooldown_hours)
        suppressed = any(
            event.event_deduplication_key == f"web:{dedup_key}"
            or (
                cooldown_hours > 0
                and event.cooldown_group == cooldown_group
                and event.event_time >= cooldown_boundary
                and (
                    (impacted and set(event.impacted_entities) & set(impacted))
                    or (not impacted and event.wake_scope == WakeScope.POOL)
                )
                and (event.source_ref == top_item.url or event.parent_event_id == evidence_signature)
            )
            for event in historical_events
        )
        if suppressed:
            continue
        seen.add(dedup_key)
        confidence = min(
            0.98,
            max(
                0.52,
                round(
                    top_item.relevance_score * 0.64
                    + top_item.source_score * 0.3
                    + 0.03
                    + (0.05 if top_item.source_tier == "official" else 0.02 if top_item.source_tier == "tier1_media" else 0.0),
                    2,
                ),
            ),
        )
        event = TriggerEventRecord(
            event_id=make_id("evt"),
            event_type=event_type,
            event_time=top_item.published_at,
            source_type=SourceType.NEWS,
            source_ref=top_item.url,
            event_deduplication_key=f"web:{dedup_key}",
            wake_scope=WakeScope.ENTITY if impacted else WakeScope.POOL,
            impacted_entities=impacted,
            trigger_confidence=confidence,
            source_quality_score=top_item.source_score,
            cooldown_group=cooldown_group,
            parent_event_id=evidence_signature,
        )
        events.append(event)
        historical_events.append(event)
    return events
