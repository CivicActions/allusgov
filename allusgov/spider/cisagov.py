import csv
from typing import Any, Iterator

import scrapy
from scrapy.http import Response


class CisagovSpider(scrapy.Spider):
    name: str = "cisagov"
    allowed_domains: list = ["githubusercontent.com"]
    start_urls: list[str] = [
        "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv"
    ]

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    @staticmethod
    def parse(response: Response) -> Iterator[Any]:
        items: dict[str, Any] = {}
        for row in csv.DictReader(response.text.splitlines()):
            item: dict[str, Any] = {}
            for key, value in row.items():
                item[key.lower().replace(" ", "_")] = value
            name: str = item.get("organization_name", "unknown")
            parent: str | None = item.get("agency", "")
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
            if name not in items:
                items[name] = {"name": name, "parent": parent, "records": []}
            items[name]["records"].append(item)
        for item in items.values():
            yield item
