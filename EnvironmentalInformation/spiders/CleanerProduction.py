import scrapy


class CleanerproductionSpider(scrapy.Spider):
    name = 'CleanerProduction'
    allowed_domains = ['xxgk.eic.sh.cn']
    start_urls = ['http://xxgk.eic.sh.cn/']

    def parse(self, response):
        pass
