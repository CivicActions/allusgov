"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/gov#license.
"""

from __future__ import annotations

from typing import Any, Callable

from loguru import logger


class Registry:
    def __init__(self) -> None:
        self._items: dict[str, type[Any]] = {}

    def register(self, key: str) -> Callable[[type[Any]], type[Any]]:
        def _decorator(cls: type[Any]) -> type[Any]:
            if key in self._items:
                logger.error("Key '{}' is already registered", key)
                raise KeyError(
                    f"Duplicate registration for '{key}': {self._items[key]} vs {cls}"
                )
            self._items[key] = cls
            return cls

        return _decorator

    def get(self, key: str) -> type[Any]:
        if key not in self._items:
            logger.error("Key '{}' is not registered: {}", key, sorted(self._items))
            raise KeyError(f"Unknown key '{key}', available: {sorted(self._items)}")
        return self._items[key]

    def keys(self) -> list[str]:
        return sorted(self._items.keys())


IMPORTERS = Registry()
EXPORTERS = Registry()
