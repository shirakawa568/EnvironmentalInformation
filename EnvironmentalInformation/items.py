# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EnvironmentalInformationItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class EnterprisesItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    area = scrapy.Field()
    type = scrapy.Field()
    url_id = scrapy.Field()


class EnterprisesDetailItem(scrapy.Item):
    dict_detail = scrapy.Field()


# 排放信息 -
class PollutionInfoItem(scrapy.Item):
    dict_pfk = scrapy.Field()
    dict_poll_project = scrapy.Field()
    dict_pfzl = scrapy.Field()
    images = scrapy.Field()


class EnterprisesImageItem(scrapy.Item):
    category = scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()


class PollutionControlFacilitiesItem(scrapy.Item):
    product = scrapy.Field()
    pullication = scrapy.Field()
    pullicationEmissions = scrapy.Field()


class AdministrativeLicensingItem(scrapy.Item):
    data = scrapy.Field()
    wryCode = scrapy.Field()
    _type = scrapy.Field()


class EmergencyPlanItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()


# 环保奖励情况
class OtherInformationRewardItem(scrapy.Item):
    dict_data = scrapy.Field()
    awards = scrapy.Field()
    rewardTime = scrapy.Field()
    awardDepartment = scrapy.Field()


class OtherInformationItem(scrapy.Item):
    dict_data = scrapy.Field()
    title = scrapy.Field()
    filename = scrapy.Field()
    uploadTime = scrapy.Field()
    url = scrapy.Field()


class OtherInformationFileItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()


class MonitoringDataItem(scrapy.Item):
    dict_data = scrapy.Field()
    title = scrapy.Field()
    url_id = scrapy.Field()


class ManualMonitoringItem(scrapy.Item):
    dict_data = scrapy.Field()
    title = scrapy.Field()
    url_id = scrapy.Field()


class EnvironmentalReportItem(scrapy.Item):
    dict_data = scrapy.Field()
    title = scrapy.Field()
    url_id = scrapy.Field()


class FilesDownloadItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()
