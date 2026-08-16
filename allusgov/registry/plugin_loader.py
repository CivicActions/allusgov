"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/gov#license.
"""

from __future__ import annotations

import importlib
import pkgutil
from types import ModuleType
from typing import Iterable


def _iter_modules(pkg: ModuleType, recursive: bool = False) -> Iterable[str]:
    # prefix ensures absolute module names suitable for import_module() [web:31]
    for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        yield name
        # If recursive and this is a subpackage, recurse into it
        if recursive and is_pkg:
            try:
                subpkg = importlib.import_module(name)
                yield from _iter_modules(subpkg, recursive=True)
            except ImportError:
                # Skip if we can't import the subpackage
                pass


def import_all_from_package(package_name: str, recursive: bool = True) -> None:
    pkg = importlib.import_module(package_name)
    for mod_name in _iter_modules(pkg, recursive=recursive):
        importlib.import_module(mod_name)
