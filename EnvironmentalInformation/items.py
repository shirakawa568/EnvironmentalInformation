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


# 环评事中事后信息公开
class InformationDisclosureItem(scrapy.Item):
    id = scrapy.Field()
    project_name = scrapy.Field()
    area = scrapy.Field()
    current_stage = scrapy.Field()
    public_date = scrapy.Field()
    project_id = scrapy.Field()


class InformationDisclosureDetailItem(scrapy.Item):
    dict_data = scrapy.Field()
    list_files = scrapy.Field()


class CleanerProductionItem(scrapy.Item):
    crop_info = scrapy.Field()  # 公司信息
    double_super = scrapy.Field()  # 双超信息
    use_materials = scrapy.Field()  # 双有信息 - 使用有毒有害原料
    discharge_materials = scrapy.Field()  # 双有信息 - 排放有毒有害物质
    generation_disposal = scrapy.Field()  # 双有信息 - 危险废物的产生和处置


class LicenseInformationItem(scrapy.Item):
    pass
