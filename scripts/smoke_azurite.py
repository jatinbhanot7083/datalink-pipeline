"""Smoke test for AzuriteObjectStore — round-trip a blob."""

from __future__ import annotations

import tempfile
from pathlib import Path

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings


def main() -> None:
    adapters = build_adapters(load_settings(env="local"))
    store = adapters.object_store

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("hello-azurite")
        tmp = Path(f.name)

    store.put(tmp, "smoke/hello.txt")
    exists = store.exists("smoke/hello.txt")
    print(f"put OK  exists={exists}")

    dl = Path(tempfile.mkdtemp()) / "dl.txt"
    store.get("smoke/hello.txt", dl)
    content = dl.read_text()
    assert content == "hello-azurite", f"content mismatch: {content!r}"
    print("get OK  content matches")

    items = store.list("smoke/")
    print(f"list: {len(items)} item(s), first key={items[0].key}")

    store.delete("smoke/hello.txt")
    exists = store.exists("smoke/hello.txt")
    print(f"delete OK  exists={exists}")

    tmp.unlink()
    dl.unlink()
    dl.parent.rmdir()
    print("ROUND-TRIP GREEN")


if __name__ == "__main__":
    main()
