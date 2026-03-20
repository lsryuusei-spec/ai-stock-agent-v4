from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from .models import EntityProfile, SourceCollectionDiagnostic, WebResearchProviderConfig
from .runtime_env import read_env_value


CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_BASE_URL = "https://static.cninfo.com.cn/"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)
IR_KEYWORDS = ("investor", "investors", "investor relations", "investor-relations", "/ir", "ir/")
IR_PATH_CANDIDATES = (
    "/investor-relations",
    "/investors",
    "/investor",
    "/ir",
    "/en/investor-relations",
    "/en/investors",
)
COMPANY_SUFFIX_TOKENS = {
    "limited",
    "holdings",
    "holding",
    "group",
    "corp",
    "corporation",
    "company",
    "co",
    "ltd",
    "inc",
    "plc",
    "technology",
    "technologies",
}


def _normalize_market(entity: EntityProfile | None) -> str:
    if entity is None:
        return "macro"
    ticker = entity.ticker.upper()
    if ticker.endswith(".HK"):
        return "hk"
    if ticker.endswith((".SH", ".SZ", ".BJ")):
        return "cn"
    return "macro"


def _entity_keys(entity: EntityProfile) -> list[str]:
    ticker = entity.ticker.upper()
    code = ticker.split(".", 1)[0]
    return [
        entity.entity_id,
        entity.entity_id.lower(),
        ticker,
        ticker.lower(),
        code,
        code.zfill(5),
        code.zfill(6),
        entity.company_name,
        entity.company_name.lower(),
    ]


def _dedupe_records(records: list[dict[str, Any]], max_results: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        title = str(record.get("title") or record.get("announcementTitle") or "").strip()
        url = str(record.get("url") or record.get("adjunctUrl") or "").strip()
        key = (title, url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
        if len(deduped) >= max_results:
            break
    return deduped


def _is_probably_chinese(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def _domain_label(url: str) -> str:
    lowered = url.lower()
    if "cninfo.com.cn" in lowered:
        return "CNINFO"
    if "hkex" in lowered:
        return "HKEXnews"
    if "investor" in lowered or "ir." in lowered:
        return "Investor Relations"
    return "Official Source"


def _parse_html_metadata(html: str) -> tuple[str | None, str | None]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else None
    description = re.sub(r"\s+", " ", meta_match.group(1)).strip() if meta_match else None
    return title, description


def _normalize_url(url: str | None) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if "." in text and " " not in text:
        return f"https://{text.lstrip('/')}"
    return None


def _extract_url_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"(https?://[^\s)]+|www\.[^\s)]+)", str(text), flags=re.IGNORECASE)
    if not match:
        return None
    return _normalize_url(match.group(1))


def _extract_anchor_links(html: str, base_url: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for match in re.finditer(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html, flags=re.IGNORECASE | re.DOTALL):
        href = match.group(1).strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        label = re.sub(r"<[^>]+>", " ", match.group(2))
        label = re.sub(r"\s+", " ", label).strip()
        absolute = urljoin(base_url, href)
        links.append((absolute, label))
    return links


def _looks_like_ir_link(url: str, label: str) -> bool:
    haystack = f"{url} {label}".lower()
    return any(keyword in haystack for keyword in IR_KEYWORDS)


def _company_name_tokens(entity: EntityProfile) -> list[str]:
    values = [getattr(entity, "english_name", None), entity.company_name]
    tokens: list[str] = []
    for value in values:
        for token in re.findall(r"[a-z0-9]+", str(value or "").lower()):
            if len(token) <= 1 or token in COMPANY_SUFFIX_TOKENS:
                continue
            tokens.append(token)
    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _candidate_domain_stems(entity: EntityProfile) -> list[str]:
    tokens = _company_name_tokens(entity)
    if not tokens:
        return []
    stems: list[str] = []
    joined = "".join(tokens[:3])
    if joined:
        stems.append(joined)
    if len(tokens) >= 2:
        stems.append("-".join(tokens[:3]))
        stems.append("".join(tokens[:2]))
    stems.extend(tokens[:2])
    deduped: list[str] = []
    seen: set[str] = set()
    for stem in stems:
        normalized = stem.strip("-")
        if len(normalized) < 4 or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


class OfficialSourceHarvester:
    def __init__(self, config: WebResearchProviderConfig):
        self.config = config
        self.last_diagnostics: list[SourceCollectionDiagnostic] = []

    def collect(self, query: str, entity: EntityProfile | None = None) -> list[dict[str, Any]]:
        self.last_diagnostics = []
        records: list[dict[str, Any]] = []
        if self.config.file_path:
            started = time.perf_counter()
            file_records = self._collect_from_file(entity)
            self._record_diagnostic(
                target="file",
                status="ok" if file_records else "empty",
                item_count=len(file_records),
                detail="Loaded official source snapshot from local file.",
                request_ref=str(self.config.file_path),
                started=started,
            )
            records.extend(file_records)
        for target in self._source_targets():
            if target == "cninfo" and entity is not None and _normalize_market(entity) == "cn":
                records.extend(self._collect_from_cninfo(entity))
            elif target == "hkex" and entity is not None and _normalize_market(entity) == "hk":
                records.extend(self._collect_from_hkex(entity))
            elif target == "ir" and entity is not None:
                records.extend(self._collect_from_ir(entity))
        if self.config.base_url:
            started = time.perf_counter()
            http_records = self._collect_from_http(query, entity)
            self._record_diagnostic(
                target="http",
                status="ok" if http_records else "empty",
                item_count=len(http_records),
                detail="Fetched official source items from configured HTTP endpoint.",
                request_ref=self.config.base_url,
                started=started,
            )
            records.extend(http_records)
        return _dedupe_records(records, self.config.max_results)

    def _source_targets(self) -> list[str]:
        targets = self.config.provider_options.get("source_targets", [])
        if not isinstance(targets, list):
            return []
        return [str(item).strip().lower() for item in targets if str(item).strip()]

    def _collect_from_file(self, entity: EntityProfile | None) -> list[dict[str, Any]]:
        path = Path(str(self.config.file_path))
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if entity is None:
            return [item for item in payload.get("macro_theme", []) if isinstance(item, dict)]
        entity_records = payload.get("entities", {})
        for key in _entity_keys(entity):
            if key in entity_records:
                return [item for item in entity_records.get(key, []) if isinstance(item, dict)]
        market_records = payload.get("markets", {}).get(_normalize_market(entity), [])
        return [item for item in market_records if isinstance(item, dict)]

    def _collect_from_cninfo(self, entity: EntityProfile) -> list[dict[str, Any]]:
        query_terms = self._cninfo_query_terms(entity)
        records: list[dict[str, Any]] = []
        for term in query_terms:
            started = time.perf_counter()
            payload = {
                "pageNum": 1,
                "pageSize": int(self.config.provider_options.get("cninfo_page_size", max(12, self.config.max_results * 4))),
                "column": self._cninfo_column(entity),
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": term,
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": str(self.config.provider_options.get("cninfo_date_range", "")),
            }
            request = Request(
                CNINFO_QUERY_URL,
                data=urlencode(payload).encode("utf-8"),
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&checkCode=plate",
                },
                method="POST",
            )
            try:
                with urlopen(request, timeout=self.config.timeout_seconds) as response:
                    payload_json = json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
                self._record_diagnostic(
                    target="cninfo",
                    status="error",
                    item_count=0,
                    detail=f"CNINFO query failed for term '{term}'.",
                    request_ref=term,
                    started=started,
                )
                continue
            matched = 0
            for announcement in payload_json.get("announcements", []):
                if not isinstance(announcement, dict) or not self._cninfo_matches_entity(announcement, entity):
                    continue
                mapped = self._map_cninfo_announcement(announcement)
                if mapped is not None:
                    records.append(mapped)
                    matched += 1
            self._record_diagnostic(
                target="cninfo",
                status="ok" if matched else "empty",
                item_count=matched,
                detail=f"CNINFO query matched {matched} announcement(s) for '{term}'.",
                request_ref=term,
                started=started,
            )
        return records

    def _collect_from_hkex(self, entity: EntityProfile) -> list[dict[str, Any]]:
        mapping = self._mapped_urls_for_target(entity, "hkex_urls")
        if not mapping:
            self._record_diagnostic(
                target="hkex",
                status="needs_mapping",
                item_count=0,
                detail="No HKEX URL mapping configured for this entity yet.",
                request_ref=entity.ticker,
                started=None,
            )
            return []
        records: list[dict[str, Any]] = []
        for item in mapping:
            url = item["url"]
            started = time.perf_counter()
            if url.lower().endswith(".pdf"):
                records.append(
                    {
                        "title": item.get("title") or f"{entity.company_name} HKEX disclosure",
                        "snippet": item.get("snippet") or f"HKEXnews disclosure for {entity.company_name}.",
                        "url": url,
                        "source_name": "HKEXnews",
                        "published_at": item.get("published_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        "relevance_score": float(item.get("relevance_score", 0.9)),
                        "evidence_type": item.get("evidence_type", "official_disclosure"),
                    }
                )
                self._record_diagnostic(
                    target="hkex",
                    status="ok",
                    item_count=1,
                    detail="Used configured HKEX document link.",
                    request_ref=url,
                    started=started,
                )
                continue
            request = Request(
                url,
                headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
                method="GET",
            )
            try:
                with urlopen(request, timeout=self.config.timeout_seconds) as response:
                    html = response.read().decode("utf-8", "ignore")
            except (HTTPError, URLError, TimeoutError):
                self._record_diagnostic(
                    target="hkex",
                    status="error",
                    item_count=0,
                    detail="HKEX page fetch failed.",
                    request_ref=url,
                    started=started,
                )
                continue
            title, description = _parse_html_metadata(html)
            if not title and not description:
                self._record_diagnostic(
                    target="hkex",
                    status="empty",
                    item_count=0,
                    detail="HKEX page returned no parsable metadata.",
                    request_ref=url,
                    started=started,
                )
                continue
            records.append(
                {
                    "title": item.get("title") or title or f"{entity.company_name} HKEX disclosure",
                    "snippet": item.get("snippet") or description or f"HKEXnews disclosure page for {entity.company_name}.",
                    "url": url,
                    "source_name": "HKEXnews",
                    "published_at": item.get("published_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    "relevance_score": float(item.get("relevance_score", 0.9)),
                    "evidence_type": item.get("evidence_type", "official_disclosure"),
                }
            )
            self._record_diagnostic(
                target="hkex",
                status="ok",
                item_count=1,
                detail="Fetched HKEX page metadata successfully.",
                request_ref=url,
                started=started,
            )
        return records

    def _collect_from_ir(self, entity: EntityProfile) -> list[dict[str, Any]]:
        urls = self._mapped_urls_for_target(entity, "ir_urls")
        if not urls:
            urls = self._discover_ir_candidates(entity)
        if not urls:
            self._record_diagnostic(
                target="ir",
                status="missing_config",
                item_count=0,
                detail="No IR URL configured or discovered for this entity.",
                request_ref=entity.ticker,
                started=None,
            )
            return []
        records: list[dict[str, Any]] = []
        for item in urls:
            url = item["url"]
            started = time.perf_counter()
            request = Request(
                url,
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml",
                },
                method="GET",
            )
            try:
                with urlopen(request, timeout=self.config.timeout_seconds) as response:
                    html = response.read().decode("utf-8", "ignore")
            except (HTTPError, URLError, TimeoutError):
                self._record_diagnostic(
                    target="ir",
                    status="error",
                    item_count=0,
                    detail="Investor relations page fetch failed.",
                    request_ref=url,
                    started=started,
                )
                continue
            title, description = _parse_html_metadata(html)
            if not title and not description:
                self._record_diagnostic(
                    target="ir",
                    status="empty",
                    item_count=0,
                    detail="Investor relations page returned no parsable metadata.",
                    request_ref=url,
                    started=started,
                )
                continue
            records.append(
                {
                    "title": title or item.get("title") or f"{entity.company_name} investor relations update",
                    "snippet": description or item.get("snippet") or f"{entity.company_name} investor relations page.",
                    "url": url,
                    "source_name": _domain_label(url),
                    "published_at": item.get("published_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    "relevance_score": float(item.get("relevance_score", 0.83)),
                    "evidence_type": item.get("evidence_type", "official_ir"),
                }
            )
            self._record_diagnostic(
                target="ir",
                status="ok",
                item_count=1,
                detail="Fetched investor relations metadata successfully.",
                request_ref=url,
                started=started,
            )
            if len(records) >= self.config.max_results:
                break
        return records

    def _discover_ir_candidates(self, entity: EntityProfile) -> list[dict[str, Any]]:
        website = self._resolve_entity_website(entity)
        if not website:
            return []
        homepage = self._fetch_html(website)
        discovered: list[dict[str, Any]] = []
        seen: set[str] = set()
        if homepage is not None:
            html, started = homepage
            links = _extract_anchor_links(html, website)
            for url, label in links:
                if not _looks_like_ir_link(url, label):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                discovered.append(
                    {
                        "url": url,
                        "title": label or f"{entity.company_name} investor relations",
                        "snippet": f"Discovered from company website {website}.",
                    }
                )
            self._record_diagnostic(
                target="ir_discovery",
                status="ok" if discovered else "empty",
                item_count=len(discovered),
                detail="Scanned company website for investor-relations links.",
                request_ref=website,
                started=started,
            )
            if discovered:
                return discovered[: int(self.config.provider_options.get("ir_discovery_max_candidates", 6))]
        else:
            self._record_diagnostic(
                target="ir_discovery",
                status="error",
                item_count=0,
                detail="Company website fetch failed during IR discovery.",
                request_ref=website,
                started=None,
            )
        for suffix in IR_PATH_CANDIDATES:
            candidate = urljoin(website.rstrip("/") + "/", suffix.lstrip("/"))
            if candidate in seen:
                continue
            seen.add(candidate)
            discovered.append(
                {
                    "url": candidate,
                    "title": f"{entity.company_name} investor relations",
                    "snippet": f"Guessed common investor-relations path from {website}.",
                }
            )
            if len(discovered) >= int(self.config.provider_options.get("ir_discovery_max_candidates", 6)):
                break
        return discovered[: int(self.config.provider_options.get("ir_discovery_max_candidates", 6))]

    def _fetch_html(self, url: str) -> tuple[str, float] | None:
        started = time.perf_counter()
        request = Request(
            url,
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                html = response.read().decode("utf-8", "ignore")
        except (HTTPError, URLError, TimeoutError):
            return None
        return html, started

    def _resolve_entity_website(self, entity: EntityProfile) -> str | None:
        direct = _normalize_url(getattr(entity, "website", None))
        if direct:
            return direct
        override_map = self.config.provider_options.get("website_overrides", {})
        if isinstance(override_map, dict):
            for key in _entity_keys(entity):
                override = _normalize_url(override_map.get(key))
                if override:
                    return override
        from_summary = _extract_url_from_text(getattr(entity, "business_summary", None))
        if from_summary:
            return from_summary
        return self._guess_entity_website(entity)

    def _guess_entity_website(self, entity: EntityProfile) -> str | None:
        stems = _candidate_domain_stems(entity)
        if not stems:
            return None
        tlds = self.config.provider_options.get("website_tlds", ["com", "com.cn", "cn", "hk", "com.hk"])
        if not isinstance(tlds, list):
            tlds = ["com", "com.cn", "cn", "hk", "com.hk"]
        max_candidates = int(self.config.provider_options.get("website_rule_max_candidates", 6))
        attempts = 0
        for stem in stems:
            for tld in tlds:
                for prefix in ("https://www.", "https://"):
                    if attempts >= max_candidates:
                        self._record_diagnostic(
                            target="website_discovery",
                            status="empty",
                            item_count=0,
                            detail=f"No homepage candidate validated after {attempts} attempts.",
                            request_ref=entity.company_name,
                            started=None,
                        )
                        return None
                    attempts += 1
                    candidate = f"{prefix}{stem}.{tld}"
                    fetched = self._fetch_html(candidate)
                    if fetched is None:
                        continue
                    html, started = fetched
                    if self._looks_like_company_website(html, candidate, entity, stem):
                        self._record_diagnostic(
                            target="website_discovery",
                            status="ok",
                            item_count=1,
                            detail="Validated homepage candidate from company-name rules.",
                            request_ref=candidate,
                            started=started,
                        )
                        return candidate
        self._record_diagnostic(
            target="website_discovery",
            status="empty",
            item_count=0,
            detail=f"No homepage candidate validated after {attempts} attempts.",
            request_ref=entity.company_name,
            started=None,
        )
        return None

    def _looks_like_company_website(self, html: str, url: str, entity: EntityProfile, stem: str) -> bool:
        title, description = _parse_html_metadata(html)
        haystack = _normalize_text(" ".join([title or "", description or "", url]))
        company_tokens = _company_name_tokens(entity)
        if stem.replace("-", "") in haystack.replace("-", ""):
            return True
        token_hits = sum(1 for token in company_tokens[:3] if token in haystack)
        if token_hits >= 2:
            return True
        company_name = str(entity.company_name or "").strip()
        if company_name and company_name.lower() in haystack:
            return True
        return False

    def _mapped_urls_for_target(self, entity: EntityProfile, option_key: str) -> list[dict[str, Any]]:
        raw_urls = self.config.provider_options.get(option_key, {})
        if not isinstance(raw_urls, dict):
            return []
        for key in _entity_keys(entity):
            urls = raw_urls.get(key)
            if isinstance(urls, str):
                return [{"url": urls}]
            if isinstance(urls, dict) and isinstance(urls.get("url"), str):
                return [urls]
            if isinstance(urls, list):
                mapped: list[dict[str, Any]] = []
                for url in urls:
                    if isinstance(url, str) and url.strip():
                        mapped.append({"url": url})
                    elif isinstance(url, dict) and isinstance(url.get("url"), str):
                        mapped.append(url)
                return mapped
        return []

    def _collect_from_http(self, query: str, entity: EntityProfile | None) -> list[dict[str, Any]]:
        url = self.config.base_url.rstrip("/")
        if self.config.endpoint:
            url = f"{url}/{self.config.endpoint.lstrip('/')}"
        params = {"q": query, "max_results": str(self.config.max_results)}
        if entity is not None:
            params["ticker"] = entity.ticker
            params["market"] = _normalize_market(entity)
            params["entity_id"] = entity.entity_id
        request_url = f"{url}?{urlencode(params)}"
        headers = {"Accept": "application/json"}
        if self.config.auth_env_var:
            token = read_env_value(self.config.auth_env_var)
            if token:
                headers["Authorization"] = f"Bearer {token}"
        request = Request(request_url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            return []
        records = payload.get("results", payload.get("items", []))
        return [item for item in records if isinstance(item, dict)]

    def _cninfo_column(self, entity: EntityProfile) -> str:
        ticker = entity.ticker.upper()
        if ticker.endswith(".SH"):
            return "sse"
        if ticker.endswith((".SZ", ".BJ")):
            return "szse"
        return str(self.config.provider_options.get("cninfo_column", "sse"))

    def _cninfo_query_terms(self, entity: EntityProfile) -> list[str]:
        code = entity.ticker.split(".", 1)[0].zfill(6)
        aliases: list[str] = [code]
        alias_map = self.config.provider_options.get("entity_aliases", {})
        if isinstance(alias_map, dict):
            for key in _entity_keys(entity):
                alias_value = alias_map.get(key)
                if isinstance(alias_value, str) and alias_value.strip():
                    aliases.append(alias_value.strip())
                elif isinstance(alias_value, list):
                    aliases.extend(str(item).strip() for item in alias_value if str(item).strip())
        if _is_probably_chinese(entity.company_name):
            aliases.append(entity.company_name)
        deduped: list[str] = []
        seen: set[str] = set()
        for alias in aliases:
            if alias in seen:
                continue
            seen.add(alias)
            deduped.append(alias)
        return deduped

    def _cninfo_matches_entity(self, announcement: dict[str, Any], entity: EntityProfile) -> bool:
        code = entity.ticker.split(".", 1)[0].zfill(6)
        sec_code = str(announcement.get("secCode") or "").zfill(6)
        if sec_code == code:
            return True
        haystack = _normalize_text(
            " ".join(
                [
                    str(announcement.get("secName") or ""),
                    str(announcement.get("tileSecName") or ""),
                    str(announcement.get("announcementTitle") or ""),
                    str(announcement.get("shortTitle") or ""),
                ]
            )
        )
        return any(_normalize_text(key) in haystack for key in _entity_keys(entity) if len(str(key).strip()) >= 4)

    def _map_cninfo_announcement(self, announcement: dict[str, Any]) -> dict[str, Any] | None:
        title = str(announcement.get("announcementTitle") or announcement.get("shortTitle") or "").strip()
        adjunct_url = str(announcement.get("adjunctUrl") or "").strip()
        if not title:
            return None
        url = adjunct_url if adjunct_url.startswith("http") else f"{CNINFO_STATIC_BASE_URL}{adjunct_url.lstrip('/')}"
        published_at = announcement.get("announcementTime")
        if isinstance(published_at, (int, float)):
            published_text = datetime.fromtimestamp(float(published_at) / 1000, tz=UTC).isoformat().replace("+00:00", "Z")
        else:
            published_text = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        sec_name = str(announcement.get("secName") or announcement.get("tileSecName") or "Listed company").strip()
        doc_type = str(announcement.get("adjunctType") or "document").strip()
        snippet = f"{sec_name} official disclosure ({doc_type})."
        type_name = str(announcement.get("announcementTypeName") or "").strip()
        if type_name:
            snippet = f"{snippet} {type_name}."
        return {
            "title": title,
            "snippet": snippet,
            "url": url,
            "source_name": "CNINFO",
            "published_at": published_text,
            "relevance_score": 0.93,
            "evidence_type": "official_disclosure",
            "document_type": type_name or announcement.get("announcementType") or "announcement",
        }

    def _record_diagnostic(
        self,
        *,
        target: str,
        status: str,
        item_count: int,
        detail: str,
        request_ref: str | None,
        started: float | None,
    ) -> None:
        latency_ms = 0.0 if started is None else round((time.perf_counter() - started) * 1000, 2)
        self.last_diagnostics.append(
            SourceCollectionDiagnostic(
                target=target,
                status=status,
                item_count=item_count,
                detail=detail,
                request_ref=request_ref,
                latency_ms=latency_ms,
            )
        )
