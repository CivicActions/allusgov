from typing import Any, Dict, Iterator, List, Optional, Union

import scrapy
from scrapy.http.request import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.selector.unified import Selector, SelectorList


class UsagovSpider(scrapy.Spider):
    name = "usagov"
    allowed_domains = ["usa.gov"]
    base = "https://www.usa.gov"
    multiple_fields = [
        "contact",
        "local_offices",
        "phone_number",
        "toll_free",
        "tty",
        "sms",
        "website",
    ]
    link_fields = [
        "contact",
        "email",
        "forms",
        "local_offices",
        "website",
    ]

    def start_requests(self) -> Iterator[Request]:
        url = self.base + "/federal-agencies"
        yield scrapy.Request(url=url, callback=self.parse)

    def get_field(
        self, head: str, item: Union[Selector, SelectorList]
    ) -> Union[str, Dict[str, str]]:
        if head in self.link_fields:
            return {
                "title": item.css("*::text").get().strip(),
                "link": item.css("a::attr(href)").get(),
            }
        return item.css("*::text").get().strip()

    def parse(
        self, response: HtmlResponse, agency_name: Optional[str] = None, **kwargs: Any
    ) -> Iterator[
        Union[
            Request,
            Dict[str, Union[List[Dict[str, str]], str, Dict[str, str], List[str]]],
            Dict[str, Union[List[Dict[str, str]], str, List[str]]],
        ]
    ]:
        rows = response.xpath("//table//tr")

        for row in rows:
            acronym = row.xpath("td[1]/text()").get()
            definitions_raw = row.xpath("td[2]").get()

            definitions_raw = definitions_raw.split("|")
            definitions = []

            for definition_text in definitions_raw:
                temp_response = scrapy.http.HtmlResponse(
                    url="",
                    body="<html><body>" + definition_text + "</body></html>",
                    encoding="utf-8",
                )

                definition_link = temp_response.xpath("//a[1]")
                notes_links = temp_response.xpath("//i/a")

                if definition_link:
                    definition = {
                        "definition": definition_link.xpath("text()").get(),
                        "link": definition_link.xpath("@href").get(),
                    }

                    notes = []
                    for note_link in notes_links:
                        note = {
                            "note": note_link.xpath("text()").get(),
                            "link": note_link.xpath("@href").get(),
                        }
                        notes.append(note)

                    if notes:
                        definition["notes"] = notes

                    definitions.append(definition)

            yield {"acronym": acronym, "definitions": definitions}
