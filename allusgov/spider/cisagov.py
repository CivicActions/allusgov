import csv
from collections.abc import AsyncIterator, Iterator
from typing import Any

import scrapy


class CisagovSpider(scrapy.Spider):
    name: str = "cisagov"
    start_url: str = (
        "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv"
    )

    async def start(self) -> AsyncIterator[scrapy.Request]:
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(
        self, response: scrapy.http.Response, **kwargs: Any
    ) -> Iterator[dict[str, list[dict[str, str]]]]:
        items: dict[str, Any] = {}
        for row in csv.DictReader(response.text.splitlines()):
            item: dict[str, Any] = {}
            for key, value in row.items():
                item[key.lower().replace(" ", "_")] = value
            name: str = item["organization_name"]
            parent: str | None = item["agency"]
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
            if name not in items:
                items[name] = {"name": name, "parent": parent, "records": []}
            items[name]["records"].append(item)
        for item in items.values():
            yield item
