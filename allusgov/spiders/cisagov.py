import csv
from typing import Any, Dict, Iterator, List, Optional

import scrapy


class CisagovSpider(scrapy.Spider):
    name: str = "cisagov"
    start_url: str = (
        "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv"
    )

    def start_requests(self) -> scrapy.Request:
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(
        self, response: scrapy.http.Response, **kwargs: Any
    ) -> Iterator[Dict[str, List[Dict[str, str]]]]:
        items: Dict[str, Any] = {}
        for row in csv.DictReader(response.text.splitlines()):
            item: Dict[str, Any] = {}
            for key, value in row.items():
                item[key.lower().replace(" ", "_")] = value
            name: str = item["organization"]
            parent: Optional[str] = item["agency"]
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
            if name not in items:
                items[name] = {"name": name, "parent": parent, "records": []}
            items[name]["records"].append(item)
        for item in items.values():
            yield item
