import scrapy
import uuid
import datetime
import fitz  # PyMuPDF
import copy
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
        "https://www.kipi.go.ke/public-complaints",
        "https://www.kipi.go.ke/road-safety",
        "https://www.kipi.go.ke/bottom-up-economic-transformation-agenda",
        "https://www.kipi.go.ke/national-cohesion-and-values",
        "https://www.kipi.go.ke/patents",
        "https://www.kipi.go.ke/trade-marks",
        "https://www.kipi.go.ke/utility-models",
        "https://www.kipi.go.ke/industrial-designs",
        "https://www.kipi.go.ke/inventor-assistance",
        "https://www.kipi.go.ke/faqs"
    ]

    def parse(self, response):
        self.logger.info(f"Parsing: {response.url}")
        title = response.css("h1::text, h2::text").get(default="Untitled").strip()
        pdf_links = response.css("a[href$='.pdf']::attr(href)").getall()

        base_item = DocumentItem()
        base_item["id"] = str(uuid.uuid4())
        base_item["url"] = response.url
        base_item["scraper"] = "kipi_docs"
        base_item["version"] = "v1"
        base_item["name"] = title
        base_item["timestamp"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["ingested_at"] = datetime.datetime.now(datetime.timezone.utc)
        base_item["path"] = response.url.replace("https://www.kipi.go.ke/", "")
        base_item["data"] = {
            "section": base_item["path"],
            "title": title,
            "text": response.css("article, .content, .container").xpath("string()")
                                 .get(default="").strip()
        }

        self.logger.info(f"YIELDING ITEM: {base_item['name']} → {base_item['url']}")
        yield base_item

        for link in pdf_links:
            abs_url = urljoin(response.url, link)
            yield scrapy.Request(
                abs_url,
                callback=self.parse_pdf,
                meta={"item": copy.deepcopy(base_item)},
                dont_filter=True
            )

    def parse_pdf(self, response):
        item = copy.deepcopy(response.meta["item"])

        item["file_bytes"] = response.body
        item["file_content_type"] = response.headers.get("Content-Type", b"application/pdf").decode()

        try:
            with fitz.open(stream=response.body, filetype="pdf") as doc:
                text = "\n\n".join(page.get_text() for page in doc)
                item["data"]["pdf_text"] = text.strip()
        except Exception as e:
            item["data"]["pdf_text"] = f"(Error extracting PDF text: {str(e)})"

        self.logger.info(f"YIELDING ITEM: {item['name']} (PDF) → {item['url']}")
        yield item
