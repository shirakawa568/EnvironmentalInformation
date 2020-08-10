"""
 -s CLOSESPIDER_ITEMCOUNT=10
 -s CLOSESPIDER_PAGECOUNT=100

"""
from scrapy import cmdline

# cmdline.execute('scrapy crawl enterprises_info'.split())  # 通过自测
# cmdline.execute('scrapy crawl enterprises_detail'.split())  # 通过自测
# cmdline.execute('scrapy crawl pollution_info'.split())  # 通过自测
# cmdline.execute('scrapy crawl PollutionControlFacilities'.split())  # 通过自测
# cmdline.execute('scrapy crawl AdministrativeLicensing'.split())  # 通过自测
# cmdline.execute('scrapy crawl EmergencyPlan'.split())
# cmdline.execute('scrapy crawl OtherInformation'.split())
# cmdline.execute('scrapy crawl MonitoringData'.split())  # 待测试
# cmdline.execute('scrapy crawl ManualMonitoring'.split())  # 待测试
# cmdline.execute('scrapy crawl EnvironmentalReport'.split())
# cmdline.execute('scrapy crawl InformationDisclosure'.split())  # 待测试
# cmdline.execute('scrapy crawl CleanerProduction'.split())  # 待测试
cmdline.execute('scrapy crawl LicenseInformation'.split())  # 待测试
