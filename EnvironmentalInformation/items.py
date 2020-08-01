# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EnvironmentalinformationItem(scrapy.Item):
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
    images = scrapy.Field()
