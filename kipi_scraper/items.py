# file: kipi_scraper/items.py

import scrapy

class DocumentItem(scrapy.Item):
    # ---- Main Document DB Fields ----
    id = scrapy.Field()                  # Deterministic UUID (Base32 hash of path or URL)
    url = scrapy.Field()                 # Page source URL
    name = scrapy.Field()                # Page title
    scraper = scrapy.Field()             # Scraper identifier (e.g., 'kipi_docs')
    version = scrapy.Field()             # Scraper version label
    timestamp = scrapy.Field()           # Time of scrape (UTC)
    ingested_at = scrapy.Field()         # Used later for ingestion tracking
    path = scrapy.Field()                # Logical section from URL path
    data = scrapy.Field()                # Extracted content (blocks, downloads, etc.)

    # ---- Blob Store Fields ----
    document_id = scrapy.Field()         # Base32 hash of file content (used as unique id)
    file_bytes = scrapy.Field()          # Raw file binary (PDF)
    file_content_type = scrapy.Field()   # Content-Type, e.g., 'application/pdf'
