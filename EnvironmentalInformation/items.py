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
    # PWXK = scrapy.Field()
    # JSXM = scrapy.Field()
    # WFJY = scrapy.Field()
    # OTHER = scrapy.Field()
