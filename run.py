"""
 -s CLOSESPIDER_ITEMCOUNT=10

"""
from scrapy import cmdline

# cmdline.execute('scrapy crawl enterprises_info'.split())
# cmdline.execute('scrapy crawl enterprises_detail'.split())
# cmdline.execute('scrapy crawl pollution_info'.split())
cmdline.execute('scrapy crawl PollutionControlFacilities -s CLOSESPIDER_ITEMCOUNT=100'.split())
# cmdline.execute('scrapy crawl AdministrativeLicensing -s CLOSESPIDER_ITEMCOUNT=30'.split())
