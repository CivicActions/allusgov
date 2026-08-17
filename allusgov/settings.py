"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from pathlib import Path

from pydantic_settings import BaseSettings

from .processor import normalize_name


class Settings(BaseSettings):
    BASE_PATH: Path = Path(__file__).resolve().parent
    MERGE_BASE: str = "samgov"
    DATA_DIR: str = "data"
    CACHE_DIR: str = ".cache"
    POST_BUILD_PROCESSORS: list = [normalize_name.NormalizeName]


settings = Settings()
