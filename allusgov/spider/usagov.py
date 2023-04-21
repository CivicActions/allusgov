from typing import Any, Dict, Iterator, List, Union

import scrapy
from scrapy.http.request import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.selector.unified import Selector, SelectorList


class UsagovSpider(scrapy.Spider):
    # This site codebase is open source, this page has info on this section:
    # https://github.com/usagov/usagov-2021/blob/dev/docs/Federal_Directory.md
    name = "usagov"
    allowed_domains = ["usa.gov"]
    base = "https://www.usa.gov"
    start_url = base + "/agency-index"

    def start_requests(self) -> Iterator[Request]:
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def get_field(
        self, item: Union[Selector, SelectorList]
    ) -> Union[str, Dict[str, str]]:
        text = item.css("*::text").get()
        link = item.css("a::attr(href)").get()
        if link is not None:
            link_field = {}
            link_field["link"] = link
            if text is not None:
                link_field["title"] = text.strip()
            return link_field
        field = ""
        if text is not None:
            field = text.strip()
        return field

    def parse(self, response: HtmlResponse, **kwargs) -> Iterator[Request]:
        for page in response.css(".usagov-directory-container-az > li > a::attr(href)"):
            yield response.follow(url=page, callback=self.parse_directory)

    def parse_directory(self, response: HtmlResponse) -> Iterator[Request]:
        for agency in response.css(
            "div.usa-accordion > div.usa-accordion__content div:last-child > p > a::attr(href)"
        ):
            yield response.follow(
                url=agency.get(),
                callback=self.parse_agency,
            )

    def parse_agency(
        self, response: HtmlResponse, **kwargs: Any
    ) -> Iterator[
        Union[
            Request,
            Dict[str, Union[List[Dict[str, str]], str, Dict[str, str], List[str]]],
            Dict[str, Union[List[Dict[str, str]], str, List[str]]],
        ]
    ]:
        pass
        details: Dict[str, Any] = {}
        details["name"] = response.css("h1 span::text").get().strip()
        details["description"] = response.css("p.usa-intro::text").get().strip()
        for detail in response.css("div.usagov-directory-table div"):
            head = detail.css("h3.usa-prose::text").get()
            multiple = detail.css("li")
            head = head.lower().replace(" ", "_")
            # We make some assumptions about which fields are multiple and single value here.
            details[head] = []
            if multiple.get() is not None:
                for item in multiple:
                    value = self.get_field(item)
                    if value:
                        details[head].append(self.get_field(item))
            else:
                for line in detail.css("*:not(:first-child)"):
                    value = self.get_field(line)
                    if value:
                        details[head].append(self.get_field(line))
            print(details)
        if details:
            # Site no longer has parent agency field.
            # parent = response.xpath(
            #     '//section[./header/h2[text()="Parent Agency"]]/ul/li/a/text()'
            # ).get()
            # if parent is not None:
            #     details["parent"] = parent.strip()
            yield details
