"""
 -s CLOSESPIDER_ITEMCOUNT=10
 -s CLOSESPIDER_PAGECOUNT=100

"""
from scrapy import cmdline

# cmdline.execute('scrapy crawl enterprises_info'.split())  # 通过自测
# cmdline.execute('scrapy crawl enterprises_detail'.split())  # 通过自测
cmdline.execute('scrapy crawl pollution_info'.split())  # 通过自测，待确认更新数据字典表情况
# cmdline.execute('scrapy crawl PollutionControlFacilities'.split())  # 通过自测
# cmdline.execute('scrapy crawl AdministrativeLicensing'.split())  # 通过自测
# cmdline.execute('scrapy crawl EmergencyPlan'.split())  # 通过自测
# cmdline.execute('scrapy crawl OtherInformation'.split())  # 通过自测
# cmdline.execute('scrapy crawl MonitoringData'.split())  # 通过自测
# cmdline.execute('scrapy crawl ManualMonitoring'.split())  # 没有数据？
# cmdline.execute('scrapy crawl EnvironmentalReport'.split())  # 通过自测
# cmdline.execute('scrapy crawl InformationDisclosure'.split())  # 通过自测
# cmdline.execute('scrapy crawl CleanerProduction'.split())  # 通过自测
# cmdline.execute('scrapy crawl LicenseInformation'.split())  # 未完成，目前仅有企业列表与一级子菜单，未测试
