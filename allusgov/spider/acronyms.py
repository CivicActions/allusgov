import warnings
from typing import Any, Dict, Iterator, List, cast

import pandas as pd
import scrapy
import tabula
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from scrapy.http.request import Request
from scrapy.http.response import Response


class GovSpeakAcronymsSpider(scrapy.Spider):
    name = "govspeak"
    allowed_domains = ["ucsd.libguides.com"]
    start_urls = ["http://ucsd.libguides.com/"]

    def start_requests(self) -> Iterator[Request]:
        yield scrapy.Request(
            url="https://ucsd.libguides.com/govspeak/pagea",
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(self, response: Response, **kwargs) -> Iterator[Dict[str, Any]]:
        pages = response.css(".nav > li")
        for page in pages:
            yield response.follow(
                page.css("a::attr(href)").get(),
                callback=self.parse,
                headers={"User-Agent": "Mozilla/5.0"},
            )
        rows = response.xpath("//table//tr")

        for row in rows:
            acronym = row.xpath("td[1]/text()").get()
            expansions_raw = row.xpath("td[2]").get()

            # Use BeautifulSoup to parse the content of the <td> tag
            soup = BeautifulSoup(expansions_raw, "html.parser")

            # Get the content of the <td> tag without the opening and closing tags
            expansions_raw = "".join(str(e) for e in soup.td.contents)
            expansions_raw = expansions_raw.split("|")
            expansions = []

            for expansion_text in expansions_raw:
                # Since we are splitting on strings, we use BeautifulSoup to reparse HTML fragments
                with warnings.catch_warnings():
                    warnings.simplefilter(
                        "ignore", category=MarkupResemblesLocatorWarning
                    )
                    soup_def = BeautifulSoup(expansion_text, "html.parser")

                # Get the text and link only in the actual expansion (before the i tag)
                expansion = (
                    soup_def.get_text(strip=True, separator="|")
                    .split("|", maxsplit=1)[0]
                    .strip()
                )
                expansion_link = None
                if soup_def.a and not soup_def.a.find_parent("i"):
                    expansion_link = soup_def.a["href"]

                notes = []
                note_section = soup_def.find("i")
                if note_section:
                    note_section_content = "".join(
                        str(e) for e in note_section.contents
                    ).split(";")
                    for note_text in note_section_content:
                        with warnings.catch_warnings():
                            warnings.simplefilter(
                                "ignore", category=MarkupResemblesLocatorWarning
                            )
                            note_soup = BeautifulSoup(note_text, "html.parser")
                        note_text = note_soup.get_text().strip(" ()")
                        note_link = note_soup.a["href"] if note_soup.a else None
                        note_dict = {"note": note_text}
                        if note_link:
                            note_dict["link"] = note_link
                        notes.append(note_dict)

                expansion_dict = {"expansion": expansion}
                expansion_dict["source"] = "govspeak"
                if expansion_link:
                    expansion_dict["link"] = expansion_link
                if notes:
                    expansion_dict["notes"] = notes
                expansions.append(expansion_dict)
            yield {"acronym": acronym, "expansions": expansions}


class DoDAcronymsSpider(scrapy.Spider):
    name = "dod"

    def start_requests(self) -> Iterator[Request]:
        yield scrapy.Request(
            url="https://irp.fas.org/doddir/dod/dictionary.pdf",
            callback=self.parse,
        )

    def parse(self, response: Response, **kwargs) -> Iterator[Dict[str, Any]]:
        with open("/tmp/dod.pdf", "wb") as file:
            file.write(response.body)

        xmin = 90
        ymax = 752
        xmax = 600
        first_page = 245
        title_pages = [
            253,
            255,
            263,
            269,
            273,
            277,
            281,
            283,
            287,
            295,
            297,
            299,
            305,
            313,
            317,
            323,
            325,
            329,
            337,
            343,
            347,
            349,
            351,
            353,
            355,
        ]
        blank_pages = [
            252,
            268,
            276,
            280,
            294,
            296,
            304,
            312,
            322,
            324,
            328,
            336,
            342,
            348,
            352,
            354,
        ]
        acronyms: Dict[str, List[Dict[str, str]]] = {}
        for page in range(245, 356):
            ymin = 72
            if page in blank_pages:
                continue
            if page == first_page:
                ymin = 173
            elif page in title_pages:
                ymin = 102
            dfs = tabula.read_pdf(
                "/tmp/dod.pdf",
                pages=page,
                area=(ymin, xmin, ymax, xmax),
            )
            for df in dfs:
                df = cast(pd.DataFrame, df)
                acronym = ""
                for index, row in df.iterrows():
                    if pd.isna(row[0]):
                        continue
                    acronym = row[0]
                    expansion = row[1]
                    j = index + 1
                    try:
                        while pd.isna(df.iloc[j, 0]):
                            expansion += " " + df.iloc[j, 1]
                            j += 1
                    except IndexError:
                        pass
                    if acronym not in acronyms:
                        acronyms[acronym] = []
                    expansions = expansion.split(";")
                    for expansion in expansions:
                        acronyms[acronym].append(
                            {"expansion": expansion.strip(), "source": "dod"}
                        )
        for acronym, expansions in acronyms.items():
            yield {"acronym": acronym, "expansions": expansions}
