"""
 -s CLOSESPIDER_ITEMCOUNT=10

"""
from scrapy import cmdline

# cmdline.execute('scrapy crawl enterprises_info'.split())
# cmdline.execute('scrapy crawl enterprises_detail'.split())
# cmdline.execute('scrapy crawl pollution_info'.split())
# cmdline.execute('scrapy crawl PollutionControlFacilities'.split())  # 待测试
cmdline.execute('scrapy crawl AdministrativeLicensing'.split())
# cmdline.execute('scrapy crawl EmergencyPlan'.split())  # 待测试
