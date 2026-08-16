"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import json
from abc import ABC, abstractmethod
from typing import Any

from bigtree import Node

from allusgov import settings
from allusgov.utils.utils import BASE_PATH


class ImporterBase(ABC):

    @staticmethod
    def load_data(source: str) -> Any:
        file_path = BASE_PATH.parent / settings.DATA_DIR / source / "raw.json"
        try:
            with file_path.open("r", encoding="utf-8") as fp:
                return json.load(fp)
        except FileNotFoundError as fnfe:
            raise FileNotFoundError(f"Raw data file not found at {file_path}") from fnfe

    @abstractmethod
    def build(self, source: str) -> Node:
        raise NotImplementedError
