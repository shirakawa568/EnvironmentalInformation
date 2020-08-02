# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging

import pandas
import scrapy
from scrapy.pipelines.images import ImagesPipeline

from EnvironmentalInformation.spiders.enterprises_detail import EnterprisesDetailSpider
from EnvironmentalInformation.spiders.enterprises_info import EnterprisesInfoSpider
from common.tools import get_root_path, add_sheet

logger = logging.getLogger(__name__)


class EnvironmentalinformationPipeline:
    def process_item(self, item, spider):
        return item


class EnterprisesPipeline:
    def __init__(self):
        self.df = pandas.DataFrame(columns=['id', 'name', 'area', 'type', 'url_id'])

    def process_item(self, item, spider):
        # 处理item，返回的item会被后续Pipeline处理
        if isinstance(spider, EnterprisesInfoSpider):
            s = pandas.Series(item)
            self.df = self.df.append(s, ignore_index=True)
        return item

    def open_spider(self, spider):
        print("企事业单位列表爬虫开始")

    def close_spider(self, spider):
        if isinstance(spider, EnterprisesInfoSpider):
            # 根据ID排序
            self.df = self.df.sort_values('id', axis=0, ascending=True)
            print(self.df)
            # 保存Excel
            root_path = get_root_path('EnvironmentalInformation')
            self.df.to_excel(f'{root_path}Enterprises.xlsx', index=False)
            print("保存完成")


class EnterprisesDetailPipeline:
    def __init__(self):
        self.df = None
        self.count = 0
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + "Enterprises.xlsx")

    def process_item(self, item, spider):
        if isinstance(spider, EnterprisesDetailSpider):
            if self.df is None:
                # 初始化df，添加列名
                cols: dict = item['dict_detail'].keys()
                self.df = pandas.DataFrame(columns=cols)
            s = pandas.Series(item['dict_detail'])
            self.df = self.df.append(s, ignore_index=True)
            self.count += 1
            print(f"完成第{self.count}条")
        return item

    def close_spider(self, spider):
        if isinstance(spider, EnterprisesDetailSpider):
            add_sheet(get_root_path('EnvironmentalInformation'), "Enterprises.xlsx", "企业详细信息", self.df)
            print("保存完成")


class PollutionInfoPipeline:
    def __init__(self):
        self.df_pfk = None
        self.df_project = None
        self.df_images = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + "PollutionInfo.xlsx")
        self.counts = [0, 0, 0]
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        # print(item)
        # 处理排放口信息
        if item['dict_pfk'] is not None:
            if self.df_pfk is None:
                cols: dict = item['dict_pfk'][0].keys()
                self.df_pfk = pandas.DataFrame(columns=cols)
            for col in item['dict_pfk']:
                s = pandas.Series(col)
                self.df_pfk = self.df_pfk.append(s, ignore_index=True)
                self.counts[0] += 1
            return item
        # 处理排放项目信息
        if item['dict_poll_project'] is not None:
            if self.df_project is None:
                cols: dict = item['dict_poll_project'][0].keys()
                self.df_project = pandas.DataFrame(columns=cols)
            for col in item['dict_poll_project']:
                s = pandas.Series(col)
                self.df_project = self.df_project.append(s, ignore_index=True)
                self.counts[1] += 1
            return item
        # 处理厂区图
        # if item['images'] is not None:
        #     if self.df_images is None:
        #         cols: dict = item['images'][0].keys()
        #         self.df_images = pandas.DataFrame(columns=cols)
        #     for col in item['images']:
        #         s = pandas.Series(col)
        #         self.df_images = self.df_images.append(s, ignore_index=True)
        #         self.counts[2] += 1
        #     return item
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        print(self.counts, self.item_count)
        # 覆盖保存排放口信息
        self.df_pfk.to_excel(self.writer, sheet_name='排放口信息', index=False)
        self.writer.save()
        # 追加保存排放项目信息
        if self.df_project is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), "PollutionInfo.xlsx", "排放项目信息", self.df_project)
        # 追加保存厂区图信息
        if self.df_images is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), "PollutionInfo.xlsx", "厂区图信息", self.df_images)
        logger.info("save complete")


class ImgsPipeline(ImagesPipeline):
    # 主要重写下面三个父类方法
    def get_media_requests(self, item, info):
        if item['images'] is not None:
            yield scrapy.Request(item['images'])

    def file_path(self, request, response=None, info=None):
        img_name = request.url.split('/')[-1]
        return img_name  # 返回文件名

    def item_completed(self, results, item, info):
        return item  # 返回给下一个即将被执行的管道类
