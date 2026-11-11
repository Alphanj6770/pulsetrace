from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal

from pulsetrace.serialize import normalize


def test_result_is_json_and_a_copy() -> None:
    user_data = {"items": [1, 2, {"sku": "A"}]}
    normalized = normalize(user_data)
    user_data["items"].append("mutated later")  # type: ignore[attr-defined]
    assert normalized == {"items": [1, 2, {"sku": "A"}]}
    json.dumps(normalized, allow_nan=False)


def test_cycles_and_depth_are_bounded() -> None:
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    assert normalize(cyclic) == {"self": "[Circular]"}
    deep: object = "leaf"
    for _ in range(20):
        deep = [deep]
    flattened = json.dumps(normalize(deep, max_depth=4))
    assert "[MaxDepth]" in flattened


def test_special_values() -> None:
    normalized = normalize(
        {
            "nan": float("nan"),
            "inf": float("inf"),
            "big": 10**400,
            "bytes": b"\x00secret",
            "when": dt.datetime(2026, 11, 11, tzinfo=dt.UTC),
            "id": uuid.UUID(int=1),
            "money": Decimal("10.50"),
            1: "non-string key",
        }
    )
    json.dumps(normalized, allow_nan=False)
    assert normalized["nan"] == "nan"
    assert normalized["big"].startswith("<int with")
    assert normalized["bytes"] == "<bytes len=7>"
    assert normalized["money"] == "10.50"
    assert normalized["1"] == "non-string key"


def test_generators_are_not_consumed() -> None:
    generator = (n for n in range(3))
    normalize({"gen": generator})
    assert list(generator) == [0, 1, 2]


def test_broken_repr_is_survived() -> None:
    class Broken:
        def __repr__(self) -> str:
            raise RuntimeError("nope")

    assert "Broken" in normalize(Broken())  # reprlib substitutes a placeholder


def test_large_containers_are_truncated() -> None:
    normalized = normalize(list(range(1000)), max_items=10)
    assert len(normalized) == 11
    assert normalized[-1] == "[990 more items]"
