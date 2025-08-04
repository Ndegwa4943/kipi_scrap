# file: kipi_scraper/items.py

import scrapy

class DocumentItem(scrapy.Item):
    id = scrapy.Field()                  # UUID
    url = scrapy.Field()                 # Page source
    name = scrapy.Field()                # Title
    scraper = scrapy.Field()             # 'kipi_docs'
    version = scrapy.Field()             # Scraper version
    timestamp = scrapy.Field()           # Created/updated timestamp
    data = scrapy.Field()                # JSON metadata
    ingested_at = scrapy.Field()         # Time of scraping
    path = scrapy.Field()                # Logical document section

    # Optional file/blob storage fields
    file_bytes = scrapy.Field()          # Raw binary for PDFs
    file_content_type = scrapy.Field()   # E.g., 'application/pdf'
