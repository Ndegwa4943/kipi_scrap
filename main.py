from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from kipi_scraper.spiders.kipii import KipiSpider

process = CrawlerProcess(get_project_settings())
process.crawl(KipiSpider)
process.start()