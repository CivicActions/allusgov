import os
from scrapy.utils.project import get_project_settings


def full_name(org, source_name):
    """Return the best full name of the organization (from the given source if it exists)."""
    # If there is a name for the requested source, return that.
    attrs = org.get_attr(source_name)
    if attrs and "name" in attrs:
        return attrs["name"]
    # Otherwise, return the first name attribute found form any source.
    attrs = org.describe(exclude_prefix="_", exclude_attributes=["name"])
    return attrs[0][1]["name"]


def spider_uri_params(params, spider):
    return {**params, "spider_name": spider.name}


def scrapy_settings(data_dir, cache_dir, logger):
    """Return the Scrapy settings."""
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "allusgov.spiders.settings")
    settings = get_project_settings()
    settings.set(
        "FEEDS",
        {
            data_dir
            + "/%(spider_name)s/raw.json": {
                "format": "json",
                "encoding": "utf8",
                "overwrite": True,
            },
        },
    )
    settings.set("HTTPCACHE_DIR", cache_dir)
    settings.set("LOG_LEVEL", logger.getEffectiveLevel())
    return settings
