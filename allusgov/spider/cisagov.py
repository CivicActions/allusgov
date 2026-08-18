import csv
from collections.abc import AsyncIterator, Iterator
from typing import Any

import scrapy
from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class RecordRow:
    domain_name: str = Field(alias="Domain name")
    domain_type: str = Field(alias="Domain type")
    agency: str = Field(alias="Organization name")
    organization: str | None = Field(alias="Suborganization name")
    city: str = Field(alias="City")
    state: str = Field(alias="State")
    security_contact_email: str = Field(alias="Security contact email")


@dataclass
class CisaOrg:
    name: str
    parent: str
    records: list[RecordRow]


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

    @staticmethod
    def parse(
        response: scrapy.http.Response, **kwargs: Any
    ) -> Iterator[dict[str, list[RecordRow]]]:
        items: dict[str, Any] = {}
        for row in csv.DictReader(response.text.splitlines()):
            record = RecordRow(**row)
            name: str = record.agency
            parent: str | None = record.organization
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
            if name not in items:
                items[name] = {"name": name, "parent": parent, "records": []}
            items[name]["records"].append(record)
        for item in items.values():
            yield item
