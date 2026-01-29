import importlib
import logging
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def import_from_string(dotted_path: str) -> Any:
    """
    Import a class or method from a dotted path string, e.g. 'module.ClassName'.

    :param dotted_path: The dotted path to import.
    :return: The imported class or method.
    """
    try:
        module_path, attr_name = dotted_path.rsplit('.', 1)
    except ValueError as exc:
        logger.error(exc)
        raise ImportError(f"Invalid import path: {dotted_path}") from exc

    if not module_path.startswith("allusgov."):
        module_path = f"allusgov.{module_path}"

    module = importlib.import_module(module_path)
    return getattr(module, attr_name)

def resolve_imports(obj: Any) -> Any:
    """
    Recursively resolve import strings in a dict or list to actual objects.

    :param obj: The object to resolve imports in (dict, list, or str).
    :return: The object with imports resolved.
    """
    if isinstance(obj, dict):
        return {k: resolve_imports(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [resolve_imports(i) for i in obj]

    if isinstance(obj, str) and '.' in obj and not obj.endswith('.'):
        # Try to import if it looks like a module path
        try:
            return import_from_string(obj)
        except (ImportError, AttributeError, ModuleNotFoundError):
            return obj
    else:
        return obj

def load_yaml_with_imports(path: str) -> dict[str, Any]:
    """
    Load a YAML file and resolve any string values that look like imports.

    :param path: Path to the YAML file.
    :return: The loaded YAML data with imports resolved.
    """
    with open(path, 'r', encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return resolve_imports(data)
