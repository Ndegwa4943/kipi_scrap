# kipi_spider_structured.py
import scrapy
import datetime

import hashlib
import base64
from urllib.parse import urljoin
from kipi_scraper.items import DocumentItem

class KipiSpider(scrapy.Spider):
    name = "kipi"
    allowed_domains = ["kipi.go.ke"]
    start_urls = [
        "https://www.kipi.go.ke/laws-and-regulations",
        "https://www.kipi.go.ke/practice-notes",
        "https://www.kipi.go.ke/ip-cases-and-rulings",
        "https://www.kipi.go.ke/ip-statistics",
        "https://www.kipi.go.ke/expired-technologies",
        "https://www.kipi.go.ke/bottom-up-economic-transformation-agenda",
        "https://www.kipi.go.ke/patents",
        "https://www.kipi.go.ke/trade-marks",
        "https://www.kipi.go.ke/utility-models",
        "https://www.kipi.go.ke/industrial-designs",
        "https://www.kipi.go.ke/patent-agents",
        "https://ipsearch.kipi.go.ke/",
        "https://www.kipi.go.ke/fees-schedules",
        "https://www.kipi.go.ke/inventor-assistance",
        "https://www.kipi.go.ke/technology-and-innovation-support-centres-tisc",
        "https://www.kipi.go.ke/faqs"
    ]

    def base32_sha256(self, value: bytes | str) -> str:
        if isinstance(value, str):
            value = value.encode("utf-8")
        digest = hashlib.sha256(value).digest()
        return base64.b32encode(digest).decode("utf-8").rstrip('=')

    def parse(self, response):
        self.logger.info(f"Parsing: {response.url}")

        url_path = response.url.replace("https://www.kipi.go.ke/", "")
        document_id = self.base32_sha256(url_path)

        title = response.css("h1::text, h2::text").get(default="Untitled").strip()
        base_item = DocumentItem()
        base_item["id"] = document_id
        base_item["url"] = response.url
        base_item["scraper"] = "kipi_docs"
        base_item["version"] = "v1"
        base_item["name"] = title
        base_item["timestamp"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["ingested_at"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["path"] = url_path

        blocks = []
        for block in response.css(".elementor-widget-container, article, section"):
            heading = block.css("h2::text, h3::text, strong::text").get()
            paragraph = block.css("p::text").get()
            if heading or paragraph:
                blocks.append({
                    "heading": heading.strip() if heading else None,
                    "text": paragraph.strip() if paragraph else None
                })

        downloads = []
        for a in response.css("a[href$='.pdf']"):
            href = a.css("::attr(href)").get()
            label = a.css("::text").get()
            abs_url = urljoin(response.url, href)
            downloads.append({"label": label, "url": abs_url})

        base_item["data"] = {
            "title": title,
            "section": base_item["path"],
            "content_blocks": blocks,
            "downloads": downloads
        }

        yield base_item

        for download in downloads:
            yield scrapy.Request(
                download["url"],
                callback=self.parse_pdf,
                meta={"document_url": response.url, "label": download["label"]},
                dont_filter=True
            )

    def parse_pdf(self, response):
        label = response.meta.get("label")
        source_url = response.meta.get("document_url")

        file_bytes = response.body

        blob_item = DocumentItem()
        blob_item["file_bytes"] = file_bytes
        blob_item["file_content_type"] = response.headers.get("Content-Type", b"application/pdf").decode()
        blob_item["document_id"] = self.base32_sha256(file_bytes)
        blob_item["timestamp"] = datetime.datetime.now(datetime.timezone.utc)
        blob_item["url"] = response.url
        #blob_item["version"] = "v1"
        blob_item["path"] = source_url.replace("https://www.kipi.go.ke/", "")

        try:
            pass
        except Exception as e:
            text = f"(Error extracting PDF text: {str(e)})"

        blob_item["data"] = {
            "label": label,
            "source_url": source_url,
            "text": file_bytes
        }

        yield blob_item


