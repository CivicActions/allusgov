import os
from logging import Logger
from typing import Optional

from bigtree.node.node import Node
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings


def full_name(org: Optional[Node], source_name: str) -> str:
    """Return the best full name of the organization (from the given source if it exists)."""
    if org is None:
        return ""
    # If there is a name for the requested source, return that.
    attrs = org.get_attr(source_name)
    if attrs and "name" in attrs:
        return attrs["name"]
    # Otherwise, return the first name attribute found form any source.
    attrs = org.describe(exclude_prefix="_", exclude_attributes=["name"])
    return attrs[0][1]["name"]


def spider_uri_params(params, spider):
    return {**params, "spider_name": spider.name}


def scrapy_settings(
    data_dir: str, cache_dir: str, spider_page_limit: int, logger: Logger
) -> Settings:
    """Return the Scrapy settings."""
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "allusgov.spider.settings")
    settings = get_project_settings()
    settings.set(
        "FEEDS",
        {
            data_dir
            + "/%(spider_name)s/raw.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 2,
                "overwrite": True,
            },
        },
    )
    settings.set("HTTPCACHE_DIR", cache_dir)
    settings.set("LOG_LEVEL", logger.getEffectiveLevel())
    settings.set("CLOSESPIDER_PAGECOUNT", spider_page_limit)
    return settings


def scrapy_spider_closed(results):
    # We use a callback here to have access to the results list from the main thread.
    def callback(spider, reason):
        print(f"Spider {spider.name} closed with reason: {reason}")
        results.append((spider.name, reason))

    return callback
