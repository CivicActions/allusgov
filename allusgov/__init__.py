from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from allusgov.utils.plugin_loader import import_all_from_package

try:
    __version__ = version("allusgov")
except PackageNotFoundError:
    __version__ = "(local)"

del PackageNotFoundError
del version

_PLUGINS_LOADED = False


def load_plugins_once() -> None:
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return

    # exporters live in one package
    import_all_from_package("allusgov.exporter")

    # importers may live in two packages
    import_all_from_package("allusgov.importer")
    # import_all_from_package("allusgov.sources")

    _PLUGINS_LOADED = True
