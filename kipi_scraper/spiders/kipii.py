# file: kipi_scraper/spiders/kipi_spider.py

import scrapy
import datetime
import re
import hashlib
import base64
from urllib.parse import urljoin, unquote
from kipi_scraper.items import DocumentItem

LTREE_LABEL = re.compile(r'[^a-z0-9_]')

def ltree_labelize(text: str) -> str:
    s = unquote(text).lower().replace("-", "_").replace(" ", "_")
    s = LTREE_LABEL.sub('_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    if not s or not s[0].isalpha():
        s = 'x' + s
    return s

class KipiSpider(scrapy.Spider):
    name = "kipii"
    scraper_version = "1.0" # the scraper version
    allowed_domains = ["kipi.go.ke"]

    SECTION_MAP = {
        "industrial-design-rulings": "cases_and_rulings",
        "trade-mark-rulings": "cases_and_rulings",
        "practice-notes": "practice_notes",
        "expired-ip-technologies": "expired_ip_technologies",
        "ip-publications": "ip_publications",
        "ip-journal": "ip_journals",
    }

    XPATH_MAP = {
        "expired_ip_technologies": '//div[@property="schema:text"]//a',
        "ip_journals": '//table[@id="datatable"]//a[contains(@href,".pdf")]',
        "cases_and_rulings": '//div[@property="schema:text" and contains(@class,"field--name-body")]//ol//a[contains(translate(@href,"PDF","pdf"),".pdf")]',
        "practice_notes": '//div[@class="content-inner"]//ul/li/a',
        "ip_publications": '//div[@property="schema:text" and contains(@class,"field--name-body")]//a[contains(@href,".pdf")]'
    }

    def start_requests(self):
        urls = [
            "https://www.kipi.go.ke/industrial-design-rulings",
            "https://www.kipi.go.ke/trade-mark-rulings",
            "https://www.kipi.go.ke/practice-notes",
            "https://www.kipi.go.ke/expired-ip-technologies",
            "https://www.kipi.go.ke/ip-publications",
            "https://www.kipi.go.ke/ip-journal",
        ]
        for url in urls:
            section_key = url.split("/")[-1]
            slug = self.SECTION_MAP.get(section_key)
            if slug:
                yield scrapy.Request(url, self.parse, meta={'slug': slug})
            else:
                self.logger.warning(f"No section mapping found for start URL: {url}")

    def parse(self, response):
        slug = response.meta.get('slug')
        xpath = self.XPATH_MAP.get(slug)

        if not slug or not xpath:
            self.logger.warning(f"No mapping for URL: {response.url}")
            return

        self.logger.info(f"Parsing page: {response.url} using slug: {slug}")
        links = response.xpath(xpath)
        self.logger.info(f"Found {len(links)} links on this page.")

        for a in links:
            href = a.xpath('.//@href').get()
            text = a.xpath('normalize-space(string())').get(default="Download").strip()
            if not href:
                continue

            full_url = urljoin(response.url, href)
            # clean the name.
            filename = unquote(full_url.split("/")[-1])

            self.logger.info(f"Scheduling download: {text} -> {full_url}")
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_document,
                meta={
                    "title": text,
                    "slug": slug,
                    "name": filename,
                    "origin": response.url
                }
            )

        next_page = response.xpath('//a[contains(@class, "pager__item--next") or contains(@title, "Go to next page")]/@href').get()
        if next_page:
            next_url = urljoin(response.url, next_page)
            self.logger.info(f"Following next page: {next_url}")
            yield scrapy.Request(url=next_url, callback=self.parse, meta={'slug': slug})

    def parse_document(self, response):
        meta = response.meta
        file_bytes = response.body
        content_hash = hashlib.sha256(file_bytes).hexdigest()

        # Construct the path
        clean_name = ltree_labelize(meta.get('name'))
        raw_path = f"kipi.data.{meta.get('slug')}.{clean_name}"
        
        #3. Encode the entire path using Base32
        encoded_path = base64.b32encode(raw_path.encode('utf-8')).decode('utf-8')

        content_type = response.headers.get('Content-Type', b'application/octet-stream').decode('utf-8')

        metadata = {
            "title": meta.get("title"),
            "slug": meta.get("slug"),
            "origin": meta.get("origin"),
            "file_content_type": content_type,
            "content_hash": content_hash #Moved the hash into the metadata
        }

        yield DocumentItem(
            url=response.url,
            name=meta.get("name"),
            scraper=self.name,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            path=encoded_path, # Use the Base32 encoded path
            version=self.scraper_version,
            file_content_type=content_type,
            source_file=file_bytes,
            data=metadata
        )