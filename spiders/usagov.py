import scrapy


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

    def start_requests(self):
        url = self.base + "/federal-agencies"
        yield scrapy.Request(url=url, callback=self.parse)

    def get_field(self, head, item):
        if head in self.link_fields:
            return {
                "title": item.css("*::text").get().strip(),
                "link": item.css("a::attr(href)").get(),
            }
        return item.css("*::text").get().strip()

    def parse(self, response, agency_name=None):
        next_page = response.css("a.nextLetter::attr(href)").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)
        for agency in response.css("ul.one_column_bullet li"):
            agency_name = agency.css("a::text").get()
            agency_page = agency.css("a::attr(href)").get()
            if agency_page is not None:
                yield response.follow(
                    agency_page,
                    callback=self.parse,
                    cb_kwargs=dict(agency_name=agency_name),
                )
        details = {}
        for detail in response.css("article section"):
            head = detail.css("header h3::text").get()
            value = detail.css("p")
            if head is not None:
                head = head.strip(": ").lower().replace(" ", "_")
                # We make some assumptions about which fields are multiple and single value here.
                if head in self.multiple_fields:
                    details[head] = []
                    for item in value:
                        details[head].append(self.get_field(head, item))
                else:
                    details[head] = self.get_field(head, value)

        if details != {}:
            description = response.css("article header p::text").get()
            if description is not None:
                details["description"] = description.strip()
            parent = response.xpath(
                '//section[./header/h2[text()="Parent Agency"]]/ul/li/a/text()'
            ).get()
            if parent is not None:
                details["parent"] = parent.strip()
            details["name"] = agency_name
            yield details
