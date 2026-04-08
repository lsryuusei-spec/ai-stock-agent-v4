from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import (
    KnowledgeCollectionRecord,
    KnowledgeContextRecord,
    KnowledgeDocument,
    KnowledgeLayerInsight,
    KnowledgeSlice,
    KnowledgeWeightPolicy,
)
from .utils import clamp, make_id, model_hash


DEFAULT_KNOWLEDGE_POLICY = KnowledgeWeightPolicy(
    policy_id="knowledge_policy_cn_v1",
    macro_weight=0.62,
    consensus_weight=0.23,
    principle_weight=0.15,
    consensus_positive_cap=0.0,
    consensus_negative_floor=-0.12,
    principle_mode="light_gate",
    consensus_mode="crowding_risk_only",
    notes=[
        "Macro dominates context in CN and A-share use cases.",
        "Consensus is treated as crowding and expectation risk, not directional alpha.",
        "Principles act as light constraints rather than primary scoring inputs.",
    ],
)

MACRO_KEYWORDS = {
    "policy": (
        "policy",
        "fiscal",
        "pboc",
        "credit",
        "\u653f\u7b56",
        "\u8d22\u653f",
        "\u8d27\u5e01",
        "\u4ea7\u4e1a\u653f\u7b56",
        "\u4fe1\u7528",
        "\u76d1\u7ba1",
    ),
    "growth": (
        "growth",
        "gdp",
        "export",
        "consumption",
        "\u7ecf\u6d4e",
        "\u589e\u957f",
        "\u590d\u82cf",
        "\u6d88\u8d39",
        "\u51fa\u53e3",
        "\u5730\u4ea7",
    ),
    "liquidity": (
        "liquidity",
        "rates",
        "funding",
        "\u6d41\u52a8\u6027",
        "\u5229\u7387",
        "\u964d\u606f",
        "\u964d\u51c6",
        "\u793e\u878d",
    ),
}
CONSENSUS_KEYWORDS = {
    "crowding": (
        "consensus",
        "everyone",
        "crowded",
        "priced in",
        "\u4e00\u81f4\u9884\u671f",
        "\u4e3b\u6d41\u5224\u65ad",
        "\u5171\u8bc6",
        "\u62e5\u6324",
        "\u70ed\u95e8",
        "\u62b1\u56e2",
    ),
    "sentiment": (
        "sentiment",
        "risk appetite",
        "\u60c5\u7eea",
        "\u98ce\u9669\u504f\u597d",
        "\u70ed\u5ea6",
        "\u504f\u4e50\u89c2",
        "\u504f\u60b2\u89c2",
    ),
    "expectation": (
        "expectation",
        "surprise",
        "\u9884\u671f\u5dee",
        "\u9884\u671f\u8fc7\u9ad8",
        "\u9884\u671f\u8fc7\u4f4e",
        "\u5df2\u7ecf\u8ba1\u4ef7",
    ),
}
PRINCIPLE_KEYWORDS = {
    "valuation": (
        "valuation",
        "margin of safety",
        "\u4f30\u503c",
        "\u5b89\u5168\u8fb9\u9645",
        "\u4f4e\u4f30",
        "\u9ad8\u4f30",
    ),
    "risk": (
        "risk",
        "drawdown",
        "sizing",
        "stop loss",
        "\u98ce\u63a7",
        "\u6b62\u635f",
        "\u4ed3\u4f4d",
        "\u56de\u64a4",
    ),
    "behavior": (
        "discipline",
        "behavior",
        "\u7eaa\u5f8b",
        "\u8ddf\u98ce",
        "\u8ba4\u77e5",
        "\u60c5\u7eea\u5316",
    ),
}
POSITIVE_STANCE = (
    "bullish",
    "positive",
    "improve",
    "improving",
    "supportive",
    "\u5229\u597d",
    "\u6539\u5584",
    "\u4e50\u89c2",
    "\u770b\u591a",
)
NEGATIVE_STANCE = (
    "bearish",
    "negative",
    "risk",
    "\u5229\u7a7a",
    "\u6076\u5316",
    "\u627f\u538b",
    "\u60b2\u89c2",
    "\u770b\u7a7a",
)
DISCLAIMER_PREFIXES = (
    "disclaimer",
    "source:",
    "\u514d\u8d23\u58f0\u660e",
    "\u6765\u6e90:",
    "\u6765\u6e90\uff1a",
)
TRANSITION_MARKERS = (
    "\u4f46",
    "\u53e6\u4e00\u65b9\u9762",
    "\u4e0e\u6b64\u540c\u65f6",
    "\u6211\u7684\u6295\u8d44\u539f\u5219",
    "\u6295\u8d44\u539f\u5219",
    "\u4e3b\u6d41\u5224\u65ad",
    "\u4e00\u81f4\u9884\u671f",
    "however",
    "meanwhile",
    "my principle",
    "investment principle",
)
CAUTION_MARKERS = (
    "avoid",
    "wait",
    "discipline",
    "\u907f\u514d",
    "\u7b49\u5f85",
    "\u4e0d\u5e94",
    "\u5148\u7b49",
    "\u7eaa\u5f8b",
)
ENTITY_PATTERN = re.compile(
    r"\b(?:[03689]\d{5}\.(?:SH|SZ|BJ)|\d{5}\.HK)\b",
    flags=re.IGNORECASE,
)
SENTENCE_SPLITTER = re.compile(
    r"(?<=[.!?])\s+|(?<=[\u3002\uff01\uff1f])\s*|(?<=[;；])\s*"
)


def default_knowledge_policy() -> KnowledgeWeightPolicy:
    return DEFAULT_KNOWLEDGE_POLICY.model_copy(deep=True)


def load_knowledge_document_payload(payload_or_path: str) -> dict[str, Any]:
    path = Path(payload_or_path)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8-sig"))
    return json.loads(payload_or_path)


def load_knowledge_document_payloads(payload_or_path: str) -> list[dict[str, Any]]:
    path = Path(payload_or_path)
    if path.exists():
        if path.is_dir():
            payloads: list[dict[str, Any]] = []
            for item in sorted(path.glob("*.json")):
                payloads.extend(load_knowledge_document_payloads(str(item)))
            return payloads
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    else:
        raw = json.loads(payload_or_path)
    if isinstance(raw, list):
        return [dict(item) for item in raw]
    if isinstance(raw, dict) and isinstance(raw.get("documents"), list):
        return [dict(item) for item in raw["documents"]]
    if isinstance(raw, dict):
        return [raw]
    raise ValueError("knowledge payload batch must be a document object, array, or {documents: [...]}")


def load_text_payload(payload_or_path: str) -> str:
    path = Path(payload_or_path)
    if path.exists():
        return path.read_text(encoding="utf-8-sig")
    return payload_or_path


def normalize_document_text(text: str) -> str:
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    blocks: list[str] = []
    for block in cleaned.split("\n\n"):
        lines = [line.strip() for line in block.splitlines()]
        filtered = [
            line
            for line in lines
            if line
            and not any(line.lower().startswith(prefix) for prefix in DISCLAIMER_PREFIXES)
        ]
        if filtered:
            blocks.append(" ".join(filtered))
    return "\n\n".join(blocks).strip()


def summarize_document(cleaned_text: str, max_chars: int = 180) -> str:
    if not cleaned_text:
        return ""
    first_block = cleaned_text.split("\n\n", 1)[0]
    summary = first_block if len(first_block) >= 36 else cleaned_text[:max_chars]
    return summary[:max_chars].strip()


def normalize_topic_key(raw: str) -> str:
    lowered = raw.strip().lower()
    lowered = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    return lowered or "knowledge_topic"


def _keyword_score(text: str, keyword_map: dict[str, tuple[str, ...]]) -> tuple[str, int]:
    lowered = text.lower()
    best_label = "general"
    best_score = 0
    for label, keywords in keyword_map.items():
        score = sum(1 for keyword in keywords if keyword.lower() in lowered)
        if score > best_score:
            best_label = label
            best_score = score
    return best_label, best_score


def _layer_scores(text: str) -> dict[str, int]:
    _, macro_score = _keyword_score(text, MACRO_KEYWORDS)
    _, consensus_score = _keyword_score(text, CONSENSUS_KEYWORDS)
    _, principle_score = _keyword_score(text, PRINCIPLE_KEYWORDS)
    return {
        "macro": macro_score,
        "consensus": consensus_score,
        "principle": principle_score,
    }


def _infer_layer(text: str, layer_hint: str | None = None) -> str:
    if layer_hint in {"macro", "consensus", "principle"}:
        return layer_hint
    scores = _layer_scores(text)
    return max(scores, key=scores.get)


def _maybe_layer_hint(text: str) -> str | None:
    scores = _layer_scores(text)
    top_layer = max(scores, key=scores.get)
    return top_layer if scores[top_layer] > 0 else None


def _infer_subtype(text: str, layer: str) -> str:
    mapping = {
        "macro": MACRO_KEYWORDS,
        "consensus": CONSENSUS_KEYWORDS,
        "principle": PRINCIPLE_KEYWORDS,
    }[layer]
    subtype, score = _keyword_score(text, mapping)
    return subtype if score > 0 else "general"


def _infer_stance(text: str) -> str:
    lowered = text.lower()
    positive_score = sum(1 for keyword in POSITIVE_STANCE if keyword.lower() in lowered)
    negative_score = sum(1 for keyword in NEGATIVE_STANCE if keyword.lower() in lowered)
    if positive_score > negative_score:
        return "bullish"
    if negative_score > positive_score:
        return "bearish"
    return "neutral"


def _infer_confidence(text: str, layer: str, source_type: str) -> float:
    base = {"macro": 0.72, "consensus": 0.58, "principle": 0.62}[layer]
    if source_type in {"official", "sellside"}:
        base += 0.08
    if len(text) > 150:
        base += 0.04
    return round(clamp(base, 0.35, 0.92), 2)


def _infer_topic_tags(text: str, layer: str, subtype: str) -> list[str]:
    lowered = text.lower()
    tags = [layer, subtype]
    tag_map = {
        "china_policy": ("china", "\u4e2d\u56fd", "\u653f\u7b56", "\u4e24\u4f1a", "\u56fd\u5e38\u4f1a"),
        "a_share_sentiment": ("a-share", "a share", "\u4e3b\u7ebf", "\u9898\u6750", "\u6e38\u8d44"),
        "liquidity": ("liquidity", "rates", "\u6d41\u52a8\u6027", "\u793e\u878d", "\u964d\u606f", "\u964d\u51c6"),
        "valuation_framework": ("valuation", "margin of safety", "\u4f30\u503c", "\u5b89\u5168\u8fb9\u9645"),
        "risk_management": ("risk", "drawdown", "\u98ce\u63a7", "\u56de\u64a4", "\u4ed3\u4f4d", "\u6b62\u635f"),
    }
    for tag, keywords in tag_map.items():
        if any(keyword.lower() in lowered for keyword in keywords):
            tags.append(tag)
    return sorted(set(tags))


def _infer_market_scope(text: str, layer: str) -> str:
    if ENTITY_PATTERN.search(text):
        return "entity"
    if layer == "macro":
        return "macro"
    return "market"


def _infer_crowding_score(text: str, layer: str) -> float:
    if layer != "consensus":
        return 0.0
    lowered = text.lower()
    score = 0.14
    if any(
        keyword.lower() in lowered
        for keyword in (
            "consensus",
            "crowded",
            "priced in",
            "\u4e00\u81f4\u9884\u671f",
            "\u4e3b\u6d41\u5224\u65ad",
            "\u5171\u8bc6",
            "\u62e5\u6324",
        )
    ):
        score += 0.32
    if any(
        keyword.lower() in lowered
        for keyword in ("everyone", "\u70ed\u95e8", "\u62b1\u56e2", "\u8fc7\u70ed", "\u8fc7\u5ea6\u4ea4\u6613")
    ):
        score += 0.2
    return round(clamp(score, 0.0, 0.95), 2)


def _infer_principle_type(text: str, layer: str, subtype: str) -> str | None:
    if layer != "principle":
        return None
    return subtype


def _infer_action_binding(layer: str) -> str:
    if layer == "principle":
        return "light_gate"
    if layer == "consensus":
        return "risk_overlay"
    return "context_signal"


def _extract_entity_tags(text: str) -> list[str]:
    return sorted(set(match.upper() for match in ENTITY_PATTERN.findall(text)))


def _build_collection_key(layer: str, subtype: str, region: str) -> str:
    return f"{region}_{layer}_{subtype}"


def _split_block_to_sentences(block: str) -> list[str]:
    return [sentence.strip() for sentence in SENTENCE_SPLITTER.split(block) if sentence.strip()]


def _is_transition_sentence(sentence: str) -> bool:
    lowered = sentence.lower()
    return any(marker.lower() in lowered for marker in TRANSITION_MARKERS)


def segment_document(cleaned_text: str, max_chars: int = 260, min_chars: int = 40) -> list[str]:
    if not cleaned_text:
        return []
    slices: list[str] = []
    for block in [item.strip() for item in cleaned_text.split("\n\n") if item.strip()]:
        sentences = _split_block_to_sentences(block)
        if not sentences:
            continue
        current: list[str] = []
        current_layer: str | None = None
        for sentence in sentences:
            sentence_layer = _maybe_layer_hint(sentence)
            current_text = " ".join(current).strip()
            current_length = len(current_text)
            should_split = bool(
                current
                and (
                    current_length + len(sentence) + 1 > max_chars
                    or (
                        sentence_layer is not None
                        and current_layer is not None
                        and sentence_layer != current_layer
                        and current_length >= min_chars
                    )
                    or (_is_transition_sentence(sentence) and current_length >= 30)
                )
            )
            if should_split:
                slices.append(current_text)
                current = [sentence]
                current_layer = sentence_layer
                continue
            current.append(sentence)
            if sentence_layer is not None:
                current_layer = sentence_layer
        if current:
            slices.append(" ".join(current).strip())
    return slices


def classify_knowledge_slice(
    *,
    text: str,
    title: str,
    source_name: str,
    source_type: str,
    region: str,
    layer_hint: str | None = None,
    published_at: datetime | None = None,
) -> KnowledgeSlice:
    layer = _infer_layer(text, layer_hint)
    subtype = _infer_subtype(text, layer)
    return KnowledgeSlice(
        slice_id=make_id("kb_slice"),
        document_id="",
        layer=layer,
        subtype=subtype,
        title=title,
        slice_text=text,
        claim=text[:220],
        stance=_infer_stance(text),
        confidence=_infer_confidence(text, layer, source_type),
        region=region,
        market_scope=_infer_market_scope(text, layer),
        topic_tags=_infer_topic_tags(text, layer, subtype),
        entity_tags=_extract_entity_tags(text),
        crowding_score=_infer_crowding_score(text, layer),
        principle_type=_infer_principle_type(text, layer, subtype),
        action_binding=_infer_action_binding(layer),
        collection_key=_build_collection_key(layer, subtype, region),
        source_type=source_type,
        source_name=source_name,
        valid_from=published_at,
    )


def build_collection_records(slices: list[KnowledgeSlice], region: str) -> list[KnowledgeCollectionRecord]:
    grouped: dict[str, list[str]] = {}
    layer_map: dict[str, str] = {}
    for item in slices:
        grouped.setdefault(item.collection_key, []).append(item.slice_id)
        layer_map[item.collection_key] = item.layer
    records: list[KnowledgeCollectionRecord] = []
    for key, slice_ids in sorted(grouped.items()):
        records.append(
            KnowledgeCollectionRecord(
                collection_id=make_id("kb_collection"),
                collection_key=key,
                topic_key=key,
                layer=layer_map[key],
                label=key.replace("_", " "),
                region=region,
                slice_ids=sorted(slice_ids),
            )
        )
    return records


def _derive_topic_key(payload: dict[str, Any], cleaned_content: str) -> str:
    explicit = str(payload.get("topic_key") or "").strip()
    if explicit:
        return normalize_topic_key(explicit)
    title = str(payload.get("title") or "").strip()
    if title:
        return normalize_topic_key(f"{payload.get('region', 'cn')}_{title}")
    return normalize_topic_key(f"{payload.get('region', 'cn')}_{cleaned_content[:40]}")


def resolve_document_version(
    *,
    document: KnowledgeDocument,
    slices: list[KnowledgeSlice],
    collections: list[KnowledgeCollectionRecord],
    existing_documents: list[KnowledgeDocument],
    existing_slices: list[KnowledgeSlice],
    existing_collections: list[KnowledgeCollectionRecord] | None = None,
) -> tuple[
    KnowledgeDocument,
    list[KnowledgeSlice],
    list[KnowledgeCollectionRecord],
    list[KnowledgeDocument],
    list[KnowledgeSlice],
    list[KnowledgeCollectionRecord],
]:
    matched_documents = [
        item
        for item in existing_documents
        if item.topic_key == document.topic_key and item.region == document.region
    ]
    if not matched_documents:
        return document, slices, collections, [], [], []

    latest = max(matched_documents, key=lambda item: (item.version, item.created_at))
    if latest.content_hash == document.content_hash:
        return latest, [], [], [], [], []

    now = datetime.now(UTC)
    updated_previous = latest.model_copy(
        update={
            "status": "superseded",
            "superseded_by_document_id": document.document_id,
        }
    )
    updated_document = document.model_copy(
        update={
            "version": latest.version + 1,
            "supersedes_document_id": latest.document_id,
        }
    )
    updated_slices = [
        item.model_copy(
            update={
                "document_id": updated_document.document_id,
                "topic_key": updated_document.topic_key,
                "version": updated_document.version,
            }
        )
        for item in slices
    ]
    updated_collections = [
        item.model_copy(update={"topic_key": updated_document.topic_key})
        for item in collections
    ]
    retired_previous_slices = [
        item.model_copy(update={"status": "retired", "valid_to": now})
        for item in existing_slices
        if item.document_id == latest.document_id and item.status != "retired"
    ]
    previous_slice_ids = {item.slice_id for item in retired_previous_slices}
    retired_previous_collections = [
        item.model_copy(update={"status": "retired", "updated_at": now})
        for item in (existing_collections or [])
        if previous_slice_ids.intersection(item.slice_ids) and item.status != "retired"
    ]
    return (
        updated_document,
        updated_slices,
        updated_collections,
        [updated_previous],
        retired_previous_slices,
        retired_previous_collections,
    )


def _slice_match_key(item: KnowledgeSlice) -> tuple[Any, ...]:
    return (
        item.layer,
        item.subtype,
        item.collection_key,
        item.principle_type or "",
        item.market_scope,
        tuple(sorted(item.entity_tags)),
        item.title.casefold(),
    )


def _slice_change_fields(previous: KnowledgeSlice, current: KnowledgeSlice) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    comparable_fields = (
        ("claim", previous.claim, current.claim),
        ("slice_text", previous.slice_text, current.slice_text),
        ("stance", previous.stance, current.stance),
        ("confidence", previous.confidence, current.confidence),
        ("crowding_score", previous.crowding_score, current.crowding_score),
        ("status", previous.status, current.status),
        ("topic_tags", previous.topic_tags, current.topic_tags),
        ("entity_tags", previous.entity_tags, current.entity_tags),
        ("action_binding", previous.action_binding, current.action_binding),
    )
    for field, old_value, new_value in comparable_fields:
        if old_value == new_value:
            continue
        changes.append(
            {
                "field": field,
                "from": old_value,
                "to": new_value,
            }
        )
    return changes


def _build_diff_takeaways(
    *,
    from_document: KnowledgeDocument,
    to_document: KnowledgeDocument,
    added_slices: list[dict[str, Any]],
    removed_slices: list[dict[str, Any]],
    changed_slices: list[dict[str, Any]],
    document_changes: list[dict[str, Any]],
) -> list[str]:
    takeaways: list[str] = []

    if added_slices or removed_slices:
        added_layers = sorted({str(item.get("layer") or "unknown") for item in added_slices})
        removed_layers = sorted({str(item.get("layer") or "unknown") for item in removed_slices})
        parts: list[str] = []
        if added_layers:
            parts.append(f"新增了 {', '.join(added_layers)} 层的观点")
        if removed_layers:
            parts.append(f"移除了 {', '.join(removed_layers)} 层的旧观点")
        if parts:
            takeaways.append("；".join(parts) + "。")

    consensus_changes = [
        item
        for item in changed_slices
        if item.get("layer") == "consensus"
    ]
    crowding_increase = False
    for item in consensus_changes:
        for change in item.get("changes", []):
            if change.get("field") == "crowding_score":
                old_value = float(change.get("from") or 0.0)
                new_value = float(change.get("to") or 0.0)
                if new_value > old_value:
                    crowding_increase = True
                    break
        if crowding_increase:
            break
    if crowding_increase:
        takeaways.append("共识层的拥挤度在上升，说明主题叙事可能从方向判断转向交易风险判断。")

    stance_changes = []
    for item in changed_slices:
        for change in item.get("changes", []):
            if change.get("field") == "stance":
                stance_changes.append((item.get("title"), change.get("from"), change.get("to")))
    if stance_changes:
        title, old_stance, new_stance = stance_changes[0]
        takeaways.append(f"{title} 的立场从 {old_stance} 变为 {new_stance}，这通常意味着该 topic 的解读框架已经改变。")

    if any(item.get("field") == "summary" for item in document_changes):
        takeaways.append("文档摘要本身发生了变化，说明这不是单纯的元数据更新，而是作者想强调的新结论发生了改变。")

    if from_document.summary != to_document.summary and not takeaways:
        takeaways.append("这个 topic 的核心表述已经更新，建议结合左右版本摘要重新检查它是否仍适合作为当前研究前提。")

    return takeaways[:4]


def build_topic_version_diff(
    *,
    documents: list[KnowledgeDocument],
    slices: list[KnowledgeSlice],
    topic_key: str,
    region: str = "cn",
    from_version: int | None = None,
    to_version: int | None = None,
) -> dict[str, Any] | None:
    matched_documents = [
        item
        for item in documents
        if item.topic_key == topic_key and item.region == region
    ]
    if len(matched_documents) < 2:
        return None

    ordered = sorted(matched_documents, key=lambda item: (item.version, item.created_at))
    available_versions = [
        {
            "document_id": item.document_id,
            "version": item.version,
            "status": item.status,
            "published_at": item.published_at,
            "created_at": item.created_at,
        }
        for item in reversed(ordered)
    ]
    to_document = next((item for item in ordered if item.version == to_version), ordered[-1]) if to_version else ordered[-1]
    previous_candidates = [item for item in ordered if item.version < to_document.version]
    if not previous_candidates:
        return None
    from_document = (
        next((item for item in previous_candidates if item.version == from_version), previous_candidates[-1])
        if from_version
        else previous_candidates[-1]
    )

    from_slices = [
        item
        for item in slices
        if item.document_id == from_document.document_id
    ]
    to_slices = [
        item
        for item in slices
        if item.document_id == to_document.document_id
    ]

    from_by_key = {_slice_match_key(item): item for item in from_slices}
    to_by_key = {_slice_match_key(item): item for item in to_slices}
    all_keys = sorted(set(from_by_key) | set(to_by_key))

    added_slices: list[dict[str, Any]] = []
    removed_slices: list[dict[str, Any]] = []
    changed_slices: list[dict[str, Any]] = []
    for key in all_keys:
        previous = from_by_key.get(key)
        current = to_by_key.get(key)
        if previous is None and current is not None:
            added_slices.append(current.model_dump(mode="json"))
            continue
        if current is None and previous is not None:
            removed_slices.append(previous.model_dump(mode="json"))
            continue
        if previous is None or current is None:
            continue
        changes = _slice_change_fields(previous, current)
        if not changes:
            continue
        changed_slices.append(
            {
                "slice_id": current.slice_id,
                "match_key": list(key),
                "title": current.title,
                "layer": current.layer,
                "subtype": current.subtype,
                "from_slice": previous.model_dump(mode="json"),
                "to_slice": current.model_dump(mode="json"),
                "changes": changes,
            }
        )

    document_changes: list[dict[str, Any]] = []
    document_fields = (
        ("title", from_document.title, to_document.title),
        ("summary", from_document.summary, to_document.summary),
        ("status", from_document.status, to_document.status),
        ("published_at", from_document.published_at, to_document.published_at),
        ("source_name", from_document.source_name, to_document.source_name),
        ("source_type", from_document.source_type, to_document.source_type),
        ("tags", from_document.tags, to_document.tags),
    )
    for field, old_value, new_value in document_fields:
        if old_value == new_value:
            continue
        document_changes.append(
            {
                "field": field,
                "from": old_value,
                "to": new_value,
            }
        )

    summary_parts = [
        f"v{from_document.version} -> v{to_document.version}",
        f"+{len(added_slices)} added",
        f"-{len(removed_slices)} removed",
        f"~{len(changed_slices)} changed",
    ]
    if document_changes:
        summary_parts.append(f"{len(document_changes)} document fields updated")

    takeaways = _build_diff_takeaways(
        from_document=from_document,
        to_document=to_document,
        added_slices=added_slices,
        removed_slices=removed_slices,
        changed_slices=changed_slices,
        document_changes=document_changes,
    )

    return {
        "topic_key": topic_key,
        "region": region,
        "title": to_document.title,
        "summary": ", ".join(summary_parts),
        "takeaways": takeaways,
        "available_versions": available_versions,
        "from_document": from_document.model_dump(mode="json"),
        "to_document": to_document.model_dump(mode="json"),
        "document_changes": document_changes,
        "added_slices": added_slices,
        "removed_slices": removed_slices,
        "changed_slices": changed_slices,
        "stats": {
            "from_version": from_document.version,
            "to_version": to_document.version,
            "added": len(added_slices),
            "removed": len(removed_slices),
            "changed": len(changed_slices),
            "document_fields_changed": len(document_changes),
            "from_slice_count": len(from_slices),
            "to_slice_count": len(to_slices),
        },
    }


def _region_compatible(slice_region: str, market: str) -> bool:
    normalized = slice_region.lower()
    if normalized in {"all", "global"}:
        return True
    market = market.lower()
    if market == "cn":
        return normalized == "cn"
    if market == "hk":
        return normalized in {"hk", "cn"}
    return normalized == market


def _freshness_weight(item: KnowledgeSlice) -> float:
    if item.valid_from is None:
        return 1.0
    age_days = max(0.0, (datetime.now(UTC) - item.valid_from).total_seconds() / 86400)
    if age_days <= 30:
        return 1.0
    if age_days <= 90:
        return 0.92
    if age_days <= 180:
        return 0.82
    return 0.7


def infer_slice_status(item: KnowledgeSlice) -> str:
    if item.valid_to is not None and item.valid_to <= datetime.now(UTC):
        return "retired"
    if item.valid_from is None:
        return item.status or "active"
    age_days = max(0.0, (datetime.now(UTC) - item.valid_from).total_seconds() / 86400)
    if age_days > 270:
        return "retired"
    if age_days > 90:
        return "stale"
    return "active"


def refresh_slice_statuses(slices: list[KnowledgeSlice]) -> list[KnowledgeSlice]:
    refreshed: list[KnowledgeSlice] = []
    for item in slices:
        refreshed.append(item.model_copy(update={"status": infer_slice_status(item)}))
    return refreshed


def _macro_theme_terms(macro_theme: str) -> set[str]:
    return {
        term
        for term in re.split(r"[^a-z0-9_]+", macro_theme.lower())
        if len(term) >= 3
    }


def _entity_aliases(values: list[str] | set[str] | tuple[str, ...]) -> set[str]:
    aliases: set[str] = set()
    for raw in values:
        normalized = str(raw).strip().lower()
        if not normalized:
            continue
        aliases.add(normalized)
        aliases.add(normalized.split(".", 1)[0])
        aliases.add(normalized.split("_", 1)[0])
        digits = "".join(char for char in normalized if char.isdigit())
        if digits:
            aliases.add(digits)
    return aliases


def _slice_relevance(item: KnowledgeSlice, macro_theme: str, entity_ids: set[str]) -> float:
    score = item.confidence * _freshness_weight(item)
    lowered = f"{item.title} {item.slice_text}".lower()
    entity_aliases = _entity_aliases(item.entity_tags)
    if any(term in lowered for term in _macro_theme_terms(macro_theme)):
        score += 0.18
    if entity_ids.intersection(entity_aliases):
        score += 0.22
    if "china_policy" in item.topic_tags:
        score += 0.08
    if item.layer == "consensus":
        score += item.crowding_score * 0.25
    return round(score, 4)


def query_knowledge_slices(
    *,
    slices: list[KnowledgeSlice],
    market: str,
    macro_theme: str = "",
    layer: str | None = None,
    topic_key: str | None = None,
    topic_tags: list[str] | None = None,
    entity_tags: list[str] | None = None,
    status: str | None = "active",
    limit: int | None = None,
) -> list[KnowledgeSlice]:
    refreshed = refresh_slice_statuses(slices)
    requested_topics = {item.lower() for item in topic_tags or []}
    requested_entities = _entity_aliases(entity_tags or [])
    filtered: list[KnowledgeSlice] = []
    for item in refreshed:
        if not _region_compatible(item.region, market):
            continue
        if layer is not None and item.layer != layer:
            continue
        if topic_key is not None and item.topic_key != topic_key:
            continue
        if status is not None and item.status != status:
            continue
        if requested_topics and not requested_topics.intersection({tag.lower() for tag in item.topic_tags}):
            continue
        if requested_entities and not requested_entities.intersection(_entity_aliases(item.entity_tags)):
            continue
        filtered.append(item)
    ranked = sorted(
        filtered,
        key=lambda item: _slice_relevance(item, macro_theme, requested_entities),
        reverse=True,
    )
    if limit is not None:
        return ranked[:limit]
    return ranked


def prepare_notebooklm_documents(
    *,
    text: str,
    region: str = "cn",
    source_name: str = "notebooklm_curated",
    source_type: str = "notebooklm_export",
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    cleaned = normalize_document_text(text)
    if not cleaned:
        return []
    candidates = segment_document(cleaned, max_chars=420, min_chars=30)
    documents: list[dict[str, Any]] = []
    counters = {"macro": 0, "consensus": 0, "principle": 0}
    for segment in candidates:
        layer = _infer_layer(segment)
        subtype = _infer_subtype(segment, layer)
        counters[layer] += 1
        title = f"{layer}_{subtype}_{counters[layer]}"
        topic_key = normalize_topic_key(f"{region}_{layer}_{subtype}_{counters[layer]}")
        documents.append(
            {
                "title": title,
                "topic_key": topic_key,
                "layer_hint": layer,
                "source_name": source_name,
                "source_type": source_type,
                "region": region,
                "document_type": f"{layer}_note",
                "published_at": published_at,
                "tags": sorted(set([layer, subtype, *(_infer_topic_tags(segment, layer, subtype))])),
                "content": segment,
            }
        )
    return documents


def _stances_to_score(items: list[KnowledgeSlice]) -> float:
    if not items:
        return 0.0
    mapping = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}
    weighted_sum = 0.0
    total_weight = 0.0
    for item in items:
        weight = max(0.2, item.confidence * _freshness_weight(item))
        weighted_sum += mapping.get(item.stance, 0.0) * weight
        total_weight += weight
    return weighted_sum / total_weight if total_weight else 0.0


def _union_topics(items: list[KnowledgeSlice]) -> list[str]:
    topics: set[str] = set()
    for item in items:
        topics.update(item.topic_tags)
    return sorted(topics)


def _build_layer_summary(items: list[KnowledgeSlice]) -> str:
    if not items:
        return "No active knowledge slices matched."
    claims = [item.claim for item in items[:2]]
    return " | ".join(claims)


def _build_macro_insight(items: list[KnowledgeSlice]) -> KnowledgeLayerInsight:
    if not items:
        return KnowledgeLayerInsight(
            layer="macro",
            score=0.0,
            confidence=0.0,
            summary="No active macro knowledge matched.",
            action_mode="context_signal",
        )
    score = round(clamp(_stances_to_score(items), -1.0, 1.0), 2)
    confidence = round(sum(item.confidence for item in items) / len(items), 2)
    risk_flags: list[str] = []
    if score <= -0.25:
        risk_flags.append("macro_headwind")
    elif score >= 0.25:
        risk_flags.append("macro_tailwind")
    return KnowledgeLayerInsight(
        layer="macro",
        score=score,
        confidence=confidence,
        summary=_build_layer_summary(items),
        slice_ids=[item.slice_id for item in items],
        matched_topics=_union_topics(items),
        action_mode="context_signal",
        risk_flags=risk_flags,
    )


def _build_consensus_insight(
    items: list[KnowledgeSlice],
    policy: KnowledgeWeightPolicy,
) -> KnowledgeLayerInsight:
    if not items:
        return KnowledgeLayerInsight(
            layer="consensus",
            score=0.0,
            confidence=0.0,
            summary="No active consensus knowledge matched.",
            action_mode=policy.consensus_mode,
        )
    weighted_crowding = sum(item.crowding_score * item.confidence for item in items)
    total_confidence = sum(item.confidence for item in items)
    average_crowding = weighted_crowding / total_confidence if total_confidence else 0.0
    score = round(clamp(-average_crowding, -1.0, policy.consensus_positive_cap), 2)
    confidence = round(sum(item.confidence for item in items) / len(items), 2)
    risk_flags: list[str] = []
    if average_crowding >= 0.65:
        risk_flags.append("high_crowding")
    elif average_crowding >= 0.45:
        risk_flags.append("crowded_consensus")
    if any(item.subtype == "expectation" for item in items):
        risk_flags.append("expectation_risk")
    return KnowledgeLayerInsight(
        layer="consensus",
        score=score,
        confidence=confidence,
        summary=_build_layer_summary(items),
        slice_ids=[item.slice_id for item in items],
        matched_topics=_union_topics(items),
        action_mode=policy.consensus_mode,
        risk_flags=risk_flags,
    )


def _build_principle_insight(
    items: list[KnowledgeSlice],
    policy: KnowledgeWeightPolicy,
) -> KnowledgeLayerInsight:
    if not items:
        return KnowledgeLayerInsight(
            layer="principle",
            score=0.0,
            confidence=0.0,
            summary="No active principle knowledge matched.",
            action_mode=policy.principle_mode,
        )
    caution_values: list[float] = []
    risk_flags: set[str] = set()
    for item in items:
        lowered = item.slice_text.lower()
        caution = 0.12 + sum(0.12 for marker in CAUTION_MARKERS if marker.lower() in lowered)
        if item.subtype in {"valuation", "risk"}:
            caution += 0.12
        caution_values.append(caution * item.confidence)
        if item.subtype == "valuation":
            risk_flags.add("valuation_discipline")
        if item.subtype == "risk":
            risk_flags.add("position_discipline")
        if any(marker.lower() in lowered for marker in ("wait", "\u7b49\u5f85", "\u4e0d\u5e94")):
            risk_flags.add("expectation_reset_wait")
    average_caution = sum(caution_values) / max(len(caution_values), 1)
    score = round(clamp(-average_caution, -1.0, 1.0), 2)
    confidence = round(sum(item.confidence for item in items) / len(items), 2)
    return KnowledgeLayerInsight(
        layer="principle",
        score=score,
        confidence=confidence,
        summary=_build_layer_summary(items),
        slice_ids=[item.slice_id for item in items],
        matched_topics=_union_topics(items),
        action_mode=policy.principle_mode,
        risk_flags=sorted(risk_flags),
    )


def build_knowledge_context(
    *,
    run_id: str,
    macro_theme: str,
    market: str,
    slices: list[KnowledgeSlice],
    policy: KnowledgeWeightPolicy,
    wake_entity_ids: list[str] | None = None,
) -> KnowledgeContextRecord:
    wake_entity_ids = wake_entity_ids or []
    macro_items = query_knowledge_slices(
        slices=slices,
        market=market,
        macro_theme=macro_theme,
        layer="macro",
        status="active",
        limit=3,
    )
    consensus_items = query_knowledge_slices(
        slices=slices,
        market=market,
        macro_theme=macro_theme,
        layer="consensus",
        entity_tags=wake_entity_ids,
        status="active",
        limit=3,
    )
    principle_items = query_knowledge_slices(
        slices=slices,
        market=market,
        macro_theme=macro_theme,
        layer="principle",
        entity_tags=wake_entity_ids,
        status="active",
        limit=3,
    )
    macro_signal = _build_macro_insight(macro_items)
    consensus_signal = _build_consensus_insight(consensus_items, policy)
    principle_signal = _build_principle_insight(principle_items, policy)
    summary = (
        f"Knowledge overlay macro={macro_signal.score:.2f}/{macro_signal.confidence:.2f}, "
        f"consensus={consensus_signal.score:.2f}/{consensus_signal.confidence:.2f}, "
        f"principle={principle_signal.score:.2f}/{principle_signal.confidence:.2f}."
    )
    return KnowledgeContextRecord(
        context_id=make_id("kb_ctx"),
        run_id=run_id,
        macro_theme=macro_theme,
        region=market,
        policy_id=policy.policy_id,
        macro_signal=macro_signal,
        consensus_signal=consensus_signal,
        principle_signal=principle_signal,
        overall_summary=summary,
    )


def ingest_knowledge_payload(
    payload: dict[str, Any],
) -> tuple[KnowledgeDocument, list[KnowledgeSlice], list[KnowledgeCollectionRecord]]:
    raw_content = str(payload.get("content") or payload.get("raw_content") or "").strip()
    if not raw_content:
        raise ValueError("knowledge payload requires non-empty content")
    cleaned = normalize_document_text(raw_content)
    published_at = payload.get("published_at")
    if isinstance(published_at, str) and published_at.strip():
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00")).astimezone(UTC)
    else:
        published = None
    document = KnowledgeDocument(
        document_id=make_id("kb_doc"),
        title=str(payload.get("title") or "Untitled knowledge document"),
        topic_key=_derive_topic_key(payload, cleaned),
        layer_hint=payload.get("layer_hint"),
        source_name=str(payload.get("source_name") or "user_knowledge_base"),
        source_type=str(payload.get("source_type") or "curated_note"),
        region=str(payload.get("region") or "cn"),
        document_type=str(payload.get("document_type") or "article"),
        published_at=published,
        raw_content=raw_content,
        cleaned_content=cleaned,
        summary=summarize_document(cleaned),
        tags=list(payload.get("tags") or []),
        content_hash=model_hash(
            {
                "title": payload.get("title"),
                "content": cleaned,
                "source_name": payload.get("source_name"),
            }
        ),
    )
    slice_items: list[KnowledgeSlice] = []
    for index, segment in enumerate(segment_document(cleaned), start=1):
        slice_items.append(
            classify_knowledge_slice(
                text=segment,
                title=f"{document.title} [{index}]",
                source_name=document.source_name,
                source_type=document.source_type,
                region=document.region,
                layer_hint=document.layer_hint,
                published_at=document.published_at,
            ).model_copy(
                update={
                    "document_id": document.document_id,
                    "topic_key": document.topic_key,
                    "version": document.version,
                }
            )
        )
    collection_records = build_collection_records(slice_items, document.region)
    return document, slice_items, collection_records
