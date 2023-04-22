from typing import Dict, List, Any, Callable

import scrapy
from scrapy.http import JsonRequest
from w3lib.url import add_or_replace_parameters


class DigitalRegistrySpider(scrapy.Spider):
    name = "digitalregistry"

    base_url = "https://api.gsa.gov/analytics/touchpoints/v1"

    def start_requests(self):
        yield self.api_request("digital_service_accounts")
        yield self.api_request("digital_products")

    def api_request(self, endpoint: str, page: int = 0) -> JsonRequest:
        url = f"{self.base_url}/{endpoint}.json"
        url = add_or_replace_parameters(
            url,
            {
                "API_KEY": self.settings.getdict("DOTENV")["DATAGOV_API_KEY"],
                "page": str(page),
            },
        )
        # # Need some headers to avoid CDN blocks.
        # headers = ({"User-Agent": "curl/7.87.0", "Accept": "*/*"},)
        return JsonRequest(
            url,
            callback=self.parse_registry,
            cb_kwargs={"endpoint": endpoint, "page": page},
        )

    def parse(self, response, **kwargs):
        pass

    def parse_registry(self, response, endpoint: str, page: int):
        data = response.json()
        if "data" in data:
            for item in data["data"]:
                yield item
        if "meta" in data and page <= data["meta"]["totalPages"]:
            yield self.api_request(
                endpoint,
                page=page + 1,
            )
