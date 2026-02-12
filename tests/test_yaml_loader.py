"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/ssp-toolkit#license.
"""

import types

import pytest

from allusgov.utils import yaml_loader


def test_import_from_string_invalid_path():
    with pytest.raises(ImportError):
        yaml_loader.import_from_string("NoDotHere")


def test_import_from_string_prefixes_allusgov(monkeypatch):
    fake_mod = types.SimpleNamespace(MyThing=object())

    def fake_import_module(name):
        assert name == "allusgov.some.module"
        return fake_mod

    monkeypatch.setattr(yaml_loader.importlib, "import_module", fake_import_module)

    obj = yaml_loader.import_from_string("some.module.MyThing")
    assert obj is fake_mod.MyThing


def test_import_from_string_no_double_prefix(monkeypatch):
    fake_mod = types.SimpleNamespace(MyThing=object())

    def fake_import_module(name):
        assert name == "allusgov.some.module"
        return fake_mod

    monkeypatch.setattr(yaml_loader.importlib, "import_module", fake_import_module)

    obj = yaml_loader.import_from_string("allusgov.some.module.MyThing")
    assert obj is fake_mod.MyThing


def test_resolve_imports_recurses_dict_list(monkeypatch):
    sentinel = object()

    def fake_import_from_string(s):
        assert s == "pkg.mod.Something"
        return sentinel

    monkeypatch.setattr(yaml_loader, "import_from_string", fake_import_from_string)

    data = {
        "a": "pkg.mod.Something",
        "b": ["pkg.mod.Something", 123, {"c": "pkg.mod.Something"}],
        "d": "no_dot_string",
    }
    out = yaml_loader.resolve_imports(data)

    assert out["a"] is sentinel
    assert out["b"][0] is sentinel
    assert out["b"][1] == 123
    assert out["b"][2]["c"] is sentinel
    assert out["d"] == "no_dot_string"


@pytest.mark.parametrize(
    "exc",
    [ImportError, AttributeError, ModuleNotFoundError],
)
def test_resolve_imports_leaves_string_on_import_failure(monkeypatch, exc):
    def boom(_):
        raise exc("nope")

    monkeypatch.setattr(yaml_loader, "import_from_string", boom)

    s = "pkg.mod.Something"
    assert yaml_loader.resolve_imports(s) == s


def test_resolve_imports_does_not_try_if_endswith_dot(monkeypatch):
    called = False

    def fake_import_from_string(_):
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(yaml_loader, "import_from_string", fake_import_from_string)

    assert yaml_loader.resolve_imports("pkg.mod.") == "pkg.mod."
    assert called is False


def test_load_yaml_with_imports(tmp_path, monkeypatch):
    p = tmp_path / "cfg.yaml"
    p.write_text("x: pkg.mod.Something\ny: 1\n", encoding="utf-8")

    sentinel = object()

    def fake_resolve_imports(obj):
        # Assert we got parsed YAML (a dict), then return a modified version
        assert obj == {"x": "pkg.mod.Something", "y": 1}
        return {"x": sentinel, "y": 1}

    monkeypatch.setattr(yaml_loader, "resolve_imports", fake_resolve_imports)

    out = yaml_loader.load_yaml_with_imports(str(p))
    assert out == {"x": sentinel, "y": 1}
