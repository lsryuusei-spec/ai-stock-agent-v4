from __future__ import annotations

import importlib
from typing import Any


def _pick_value(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, "", "-", "--"):
            return record[key]
    return None


def _safe_number(value: Any, default: float = 0.0) -> float:
    if value in (None, "", "-", "--"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_symbol(raw_code: Any) -> str:
    text = str(raw_code or "").upper()
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(char for char in text if char.isdigit())
    return digits.zfill(5)


def _first_row(frame: Any) -> dict[str, Any]:
    if frame is None:
        return {}
    if hasattr(frame, "to_dict"):
        try:
            records = frame.to_dict("records")
        except TypeError:
            records = frame.to_dict()
        if isinstance(records, list) and records:
            return records[0]
    if isinstance(frame, list) and frame:
        item = frame[0]
        return item if isinstance(item, dict) else {}
    if isinstance(frame, dict):
        return frame
    return {}


def _liquidity_proxy(record: dict[str, Any]) -> float:
    amount = _safe_number(
        _pick_value(
            record,
            "amount",
            "turnover",
            "\u6210\u4ea4\u989d",
            "\u6210\u4ea4\u989d(\u6e2f\u5143)",
        )
    )
    if amount > 0:
        return amount
    last_price = _safe_number(_pick_value(record, "close", "\u6700\u65b0\u4ef7", "last_price"))
    volume = _safe_number(_pick_value(record, "vol", "volume", "\u6210\u4ea4\u91cf"))
    return last_price * volume


def _normalize_sector_label(
    raw_sector: str | None,
    *,
    name: str = "",
    enname: str = "",
    description: str = "",
    board: str = "",
) -> str:
    text = " ".join(
        part.strip()
        for part in [raw_sector or "", name, enname, description, board]
        if isinstance(part, str) and part.strip()
    ).lower()
    rules = [
        ("Semiconductor", ["\u534a\u5bfc\u4f53", "chip", "foundry", "wafer", "semiconductor", "\u96c6\u6210\u7535\u8def"]),
        (
            "Internet Platform",
            [
                "\u4e92\u8054\u7f51",
                "\u8f6f\u4ef6\u670d\u52a1",
                "\u7535\u5546",
                "\u4e91\u670d\u52a1",
                "platform",
                "internet",
                "software",
                "cloud",
                "tencent",
                "alibaba",
                "meituan",
                "xiaomi",
                "jd.com",
                "baidu",
                "kuaishou",
            ],
        ),
        ("Telecom", ["\u7535\u4fe1", "\u901a\u4fe1", "carrier", "telecom", "5g", "network"]),
        ("Financials", ["\u94f6\u884c", "\u4fdd\u9669", "\u8bc1\u5238", "\u91d1\u878d", "bank", "insurance", "broker", "financial", "hsbc"]),
        ("Consumer", ["\u6d88\u8d39", "\u96f6\u552e", "\u9910\u996e", "\u5546\u8d38", "consumer", "retail", "beverage", "food"]),
        ("Healthcare", ["\u533b\u7597", "\u533b\u836f", "\u751f\u7269", "pharma", "health", "medical", "biotech"]),
        ("Energy", ["\u80fd\u6e90", "\u77f3\u6cb9", "\u7164\u70ad", "\u7535\u529b", "energy", "oil", "gas", "utility"]),
        ("Industrial", ["\u5de5\u4e1a", "\u673a\u68b0", "\u5236\u9020", "\u7269\u6d41", "\u822a\u8fd0", "industrial", "logistics", "shipping", "manufacturing"]),
        ("Real Estate", ["\u5730\u4ea7", "\u7269\u4e1a", "property", "real estate"]),
        ("Materials", ["\u91d1\u5c5e", "\u6750\u6599", "\u5316\u5de5", "materials", "chemical", "mining"]),
    ]
    for label, keywords in rules:
        if any(keyword in text for keyword in keywords):
            return label
    if "\u4e3b\u677f" in text:
        return "Main Board"
    return raw_sector or "Unknown"


def _enrich_single_record(record: dict[str, Any], akshare: Any) -> dict[str, Any]:
    enriched = dict(record)
    symbol = _normalize_symbol(_pick_value(record, "code", "symbol", "ts_code", "\u4ee3\u7801", "\u8bc1\u5238\u4ee3\u7801"))
    if not symbol:
        return enriched

    financial = {}
    company_profile = {}
    security_profile = {}

    if hasattr(akshare, "stock_hk_financial_indicator_em"):
        try:
            financial = _first_row(akshare.stock_hk_financial_indicator_em(symbol=symbol))
        except Exception:
            financial = {}
    if hasattr(akshare, "stock_hk_company_profile_em"):
        try:
            company_profile = _first_row(akshare.stock_hk_company_profile_em(symbol=symbol))
        except Exception:
            company_profile = {}
    if hasattr(akshare, "stock_hk_security_profile_em"):
        try:
            security_profile = _first_row(akshare.stock_hk_security_profile_em(symbol=symbol))
        except Exception:
            security_profile = {}

    latest_price = _safe_number(
        _pick_value(record, "close", "\u6700\u65b0\u4ef7", "last_price"),
        0.0,
    )
    shares_outstanding = _safe_number(
        _pick_value(
            financial,
            "\u5df2\u53d1\u884c\u80a1\u672c(\u80a1)",
            "\u6cd5\u5b9a\u80a1\u672c(\u80a1)",
            "shares_outstanding",
        ),
        0.0,
    )
    exact_market_cap = _safe_number(
        _pick_value(
            financial,
            "\u603b\u5e02\u503c(\u6e2f\u5143)",
            "\u603b\u5e02\u503c",
            "market_cap",
        ),
        0.0,
    )
    estimated_market_cap = latest_price * shares_outstanding if latest_price > 0 and shares_outstanding > 0 else 0.0
    final_market_cap = exact_market_cap or estimated_market_cap or _safe_number(
        _pick_value(record, "market_cap", "total_mv", "marketValue"),
        0.0,
    )
    if final_market_cap > 0:
        enriched["market_cap"] = round(final_market_cap, 2)
        enriched["market_cap_quality"] = "exact" if exact_market_cap > 0 else "estimated"
    if shares_outstanding > 0:
        enriched["shares_outstanding"] = shares_outstanding
    if financial:
        pe_value = _safe_number(_pick_value(financial, "\u5e02\u76c8\u7387", "pe", "pe_ttm"), 0.0)
        pb_value = _safe_number(_pick_value(financial, "\u5e02\u51c0\u7387", "pb"), 0.0)
        if pe_value > 0:
            enriched["pe"] = pe_value
        if pb_value > 0:
            enriched["pb"] = pb_value

    raw_sector = str(
        _pick_value(
            company_profile,
            "\u6240\u5c5e\u884c\u4e1a",
            "industry",
            "\u677f\u5757",
            "sector",
        )
        or _pick_value(record, "sector", "industry", "market")
        or ""
    )
    description = str(_pick_value(company_profile, "\u516c\u53f8\u4ecb\u7ecd", "description") or "")
    board = str(_pick_value(security_profile, "\u677f\u5757", "market") or "")
    sector = _normalize_sector_label(
        raw_sector,
        name=str(_pick_value(record, "name", "\u4e2d\u6587\u540d\u79f0", "company_name") or ""),
        enname=str(
            _pick_value(
                company_profile,
                "\u82f1\u6587\u540d\u79f0",
                "enname",
                "\u82f1\u6587\u540d\u79f0",
                "\u82f1\u6587\u540d\u79f0",
            )
            or _pick_value(record, "\u82f1\u6587\u540d\u79f0", "enname")
            or ""
        ),
        description=description,
        board=board,
    )
    if sector:
        enriched["sector"] = sector
    if raw_sector:
        enriched["raw_sector"] = raw_sector
    if description:
        enriched["business_summary"] = description
    if board:
        enriched["listing_board"] = board
    if company_profile:
        website = _pick_value(company_profile, "\u516c\u53f8\u7f51\u5740", "website")
        if website:
            enriched["website"] = website
    return enriched


def enrich_hk_records(records: list[dict[str, Any]], *, max_symbols: int = 18) -> list[dict[str, Any]]:
    if not records:
        return records
    try:
        akshare = importlib.import_module("akshare")
    except ImportError:
        return records

    normalized_max = max(1, int(max_symbols))
    sorted_indices = sorted(
        range(len(records)),
        key=lambda index: _liquidity_proxy(records[index]),
        reverse=True,
    )
    selected = set(sorted_indices[:normalized_max])
    enriched_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if index in selected:
            enriched_records.append(_enrich_single_record(record, akshare))
        else:
            enriched_records.append(record)
    return enriched_records
