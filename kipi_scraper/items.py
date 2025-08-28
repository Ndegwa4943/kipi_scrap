# file: kipi_scraper/items.py

import scrapy

class DocumentItem(scrapy.Item):
    """
    Defines the structure of the data to be scraped.
    Maps directly to the 'documents' and 'scraper_blob_store' tables.
    """

    # Fields for the 'documents' table
    url = scrapy.Field()              # Source URL of the document
    scraper = scrapy.Field()          # Spider or tool that scraped the item
    version = scrapy.Field()          # SHA-256 hash of the document content
    name = scrapy.Field()             # Original file name (e.g., abc.pdf)
    timestamp = scrapy.Field()        # When it was scraped
    path = scrapy.Field()             # ltree (e.g., kipi.practice_notes.notice_pdf)
    title = scrapy.Field()
    section = scrapy.Field()
    slug = scrapy.Field()
    data = scrapy.Field()             # Metadata to power JibuDocs search

    # Fields for the 'scraper_blob_store' table
    file_content_type = scrapy.Field()  # MIME type, e.g., application/pdf
    source_file = scrapy.Field()        # Raw binary content of the file


