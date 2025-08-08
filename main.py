from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from kipi_scraper.spiders.kipi import KipiSpider

process = CrawlerProcess(get_project_settings())
process.crawl(KipiSpider)
process.start()