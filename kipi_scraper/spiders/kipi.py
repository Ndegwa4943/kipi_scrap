# kipi_spider_structured.py
import scrapy
import uuid
import datetime
import fitz
import copy
from urllib.parse import urljoin
from kipi_scraper.items import DocumentItem

class KipiSpider(scrapy.Spider):
    name = "kipi"
    allowed_domains = ["kipi.go.ke"]
    start_urls = [
        #RESOURCES urls
        "https://www.kipi.go.ke/laws-and-regulations",
        "https://www.kipi.go.ke/practice-notes",
        "https://www.kipi.go.ke/ip-cases-and-rulings",
        "https://www.kipi.go.ke/ip-statistics",
        "https://www.kipi.go.ke/expired-technologies",
        "https://www.kipi.go.ke/bottom-up-economic-transformation-agenda",

        #services urls
        "https://www.kipi.go.ke/patents",
        "https://www.kipi.go.ke/trade-marks",
        "https://www.kipi.go.ke/utility-models",
        "https://www.kipi.go.ke/industrial-designs",
        "https://www.kipi.go.ke/patent-agents",
        "https://ipsearch.kipi.go.ke/",
        "https://www.kipi.go.ke/fees-schedules",
        "https://www.kipi.go.ke/inventor-assistance",
        "https://www.kipi.go.ke/technology-and-innovation-support-centres-tisc",

        #FAQS urls
        "https://www.kipi.go.ke/faqs"
    ]

    def parse(self, response):
        self.logger.info(f"Parsing: {response.url}")
        title = response.css("h1::text, h2::text").get(default="Untitled").strip()
        base_item = DocumentItem()
        base_item["id"] = str(uuid.uuid4())
        base_item["url"] = response.url
        base_item["scraper"] = "kipi_docs"
        base_item["version"] = "v1"
        base_item["name"] = title
        base_item["timestamp"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["ingested_at"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["path"] = response.url.replace("https://www.kipi.go.ke/", "")

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
                meta={"item": copy.deepcopy(base_item), "label": download["label"]},
                dont_filter=True
            )

    def parse_pdf(self, response):
        item = copy.deepcopy(response.meta["item"])
        label = response.meta.get("label")

        item["file_bytes"] = response.body
        item["file_content_type"] = response.headers.get("Content-Type", b"application/pdf").decode()

        try:
            with fitz.open(stream=response.body, filetype="pdf") as doc:
                text = "\n\n".join(page.get_text() for page in doc)
        except Exception as e:
            text = f"(Error extracting PDF text: {str(e)})"

        item["data"].setdefault("pdfs", []).append({
            "label": label,
            "url": response.url,
            "text": text.strip()
        })

        yield item

