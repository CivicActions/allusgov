from collections.abc import AsyncIterator, Iterator
from typing import Any

import scrapy
from scrapy.http.request import Request
from scrapy.http.response.text import TextResponse


class FederalRegisterSpider(scrapy.Spider):
    name = "federalregister"
    allowed_domains = ["federalregister.gov"]
    start_url = "https://www.federalregister.gov/api/v1/agencies"

    async def start(self) -> AsyncIterator[Request]:
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    @staticmethod
    def parse(
        response: TextResponse, **kwargs: Any
    ) -> Iterator[Request | dict[str, int | str | None | float]]:
        agencies = response.json()
        for agency in agencies:
            # The JSON structure magically exactly matches almost exactly what we need!
            agency["id"] = str(agency["id"])
            if agency["parent_id"] is not None:
                agency["parent_id"] = str(agency["parent_id"])
            yield agency
