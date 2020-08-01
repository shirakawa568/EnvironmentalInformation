from scrapy import cmdline

# cmdline.execute('scrapy crawl enterprises_info'.split())
# cmdline.execute('scrapy crawl enterprises_detail -s CLOSESPIDER_ITEMCOUNT=10'.split())
cmdline.execute('scrapy crawl pollution_info'.split())
