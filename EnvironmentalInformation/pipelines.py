# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
from os.path import join, basename, dirname
from urllib.parse import urlparse

import pandas
import scrapy
from scrapy.pipelines.files import FilesPipeline
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

    def open_spider(self, spider):
        print("企事业单位列表爬虫开始")

    def process_item(self, item, spider):
        # 处理item，返回的item会被后续Pipeline处理
        if isinstance(spider, EnterprisesInfoSpider):
            s = pandas.Series(item)
            self.df = self.df.append(s, ignore_index=True)
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        if isinstance(spider, EnterprisesInfoSpider):
            # 根据ID排序
            self.df = self.df.sort_values('id', axis=0, ascending=True)
            print(self.df)
            # 保存Excel
            root_path = get_root_path('EnvironmentalInformation')
            self.df.to_excel(f'{root_path}Enterprises.xlsx', index=False)
            print("保存完成")
        logger.info("save complete")


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
        logger.info('spider is ending')
        if isinstance(spider, EnterprisesDetailSpider):
            add_sheet(get_root_path('EnvironmentalInformation'), "Enterprises.xlsx", "企业详细信息", self.df)
            print("保存完成")
        logger.info("save complete")


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
            logger.info(f"{self.counts[1]}\t收到Item\t{len(item['dict_pfk'])}条排放口")
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
            logger.info(f"{self.counts[1]}\t收到Item\t{len(item['dict_poll_project'])}条排放项目")
            return item
        # 处理排放总量信息
        if item['dict_pfzl'] is not None:
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


class DownloadImagesPipeline(ImagesPipeline):
    # 主要重写下面三个父类方法
    def get_media_requests(self, item, info):
        if item['images'] is not None:
            logger.info(f"url_id:{item['images']['url_id']}\t'url:'{item['images']['url']}")
            yield scrapy.Request(item['images']['url'])

    def file_path(self, request, response=None, info=None):
        img_name = request.url.split('/')[-1]
        return img_name  # 返回文件名

    def item_completed(self, results, item, info):
        return item  # 返回给下一个即将被执行的管道类


class PollutionControlFacilitiesPipeline:
    def __init__(self):
        self.df_product = None
        self.df_pullication = None
        self.df_pullicationEmissions = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + "PollutionControlFacilities.xlsx")
        self.counts = [0, 0, 0]
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        if item['product'] is not None:
            # 创建列名
            if self.df_product is None:
                cols: dict = item['product'][0].keys()
                self.df_product = pandas.DataFrame(columns=cols)
            for col in item['product']:
                s = pandas.Series(col)
                self.df_product = self.df_product.append(s, ignore_index=True)
                self.counts[0] += 1
            # logger.info(f"{self.counts[0]}\t收到{len(item['product'])}条产生污染设施情况")
            print(f"{self.item_count}\t{self.counts}\t收到{len(item['product'])}条产生污染设施情况")
            return item
        if item['pullication'] is not None:
            # 创建列名
            if self.df_pullication is None:
                cols: dict = item['pullication'][0].keys()
                self.df_pullication = pandas.DataFrame(columns=cols)
            for col in item['pullication']:
                s = pandas.Series(col)
                self.df_pullication = self.df_pullication.append(s, ignore_index=True)
                self.counts[1] += 1
            # logger.info(f"{self.counts[0]}\t收到{len(item['pullication'])}条污染处理设施建设运行情况")
            print(f"{self.item_count}\t{self.counts}\t收到{len(item['pullication'])}条污染处理设施建设运行情况")
            return item
        if item['pullicationEmissions'] is not None:
            # 创建列名
            if self.df_pullicationEmissions is None:
                cols: dict = item['pullicationEmissions'][0].keys()
                self.df_pullicationEmissions = pandas.DataFrame(columns=cols)
            for col in item['pullicationEmissions']:
                s = pandas.Series(col)
                self.df_pullicationEmissions = self.df_pullicationEmissions.append(s, ignore_index=True)
                self.counts[2] += 1
            # logger.info(f"{self.counts[0]}\t收到{len(item['pullicationEmissions'])}条污染物排放方式及排放去向")
            print(f"{self.item_count}\t{self.counts}\t收到{len(item['pullicationEmissions'])}条污染物排放方式及排放去向")
            return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        print("爬取内容条数：", self.counts, "激活Item数：", self.item_count)
        # 覆盖保存
        self.df_product.to_excel(self.writer, sheet_name='产生污染设施情况', index=False)
        self.writer.save()
        # 追加保存
        if self.df_pullication is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), "PollutionControlFacilities.xlsx", "污染处理设施建设运行情况",
                      self.df_pullication)
        # 追加保存
        if self.df_pullicationEmissions is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), "PollutionControlFacilities.xlsx", "污染物排放方式及排放去向",
                      self.df_pullicationEmissions)
        # 爬虫结束
        logger.info("save complete")


# 排污许可、建设项目、???、其它许可
class AdministrativeLicensingPipeline:
    def __init__(self):
        self.df_PWXK = None
        self.df_JSXM = None
        self.df_WFJY = None
        self.df_OTHER = None
        self.filename = "AdministrativeLicensing.xlsx"
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)
        self.counts = [0, 0, 0, 0]
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        wryCode = item['wryCode']
        _type = item['_type']
        data = item['data']

        # 排污许可证
        if _type == "PWXK":
            self.df_PWXK = self.test(self.df_PWXK, data, "排污许可证")
            self.counts[0] += len(data)
            return item
        # 建设项目行政办理事项
        if _type == "JSXM":
            self.df_JSXM = self.test(self.df_JSXM, data, "建设项目行政办理事项")
            self.counts[1] += len(data)
            return item
        # 未知
        if _type == "WFJY":
            self.df_WFJY = self.test(self.df_WFJY, data, "未知")
            self.counts[2] += len(data)
            return item
        # 其他行政许可事项
        if _type == "OTHER":
            self.df_OTHER = self.test(self.df_OTHER, data, "其他行政许可事项")
            self.counts[3] += len(data)
            return item

    def test(self, df, data, title):
        if df is None:
            cols: dict = data[0].keys()
            df = pandas.DataFrame(columns=cols)
        for col in data:
            s = pandas.Series(col)
            df = df.append(s, ignore_index=True)
        return df

    def close_spider(self, spider):
        logger.info('spider is ending')
        print("爬取内容条数：", self.counts, "Item总数：", self.item_count)
        # 覆盖保存
        self.df_PWXK.to_excel(self.writer, sheet_name='排污许可证', index=False)
        self.writer.save()
        # 追加保存
        if self.df_JSXM is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, "建设项目",
                      self.df_JSXM)
        # 追加保存
        if self.df_WFJY is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, "建设项目行政办理事项",
                      self.df_WFJY)
        # 追加保存
        if self.df_OTHER is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, "其他行政许可事项",
                      self.df_OTHER)
        # 爬虫结束
        logger.info("save complete")


class EmergencyPlanFilePipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        if item.get('file_urls', None):
            for i in range(len(item['file_urls'])):
                url = item['file_urls'][i]
                file = item["files"][i]
                yield scrapy.Request(url=url, meta={'filename': file})

    def file_path(self, request, response=None, info=None):
        return request.meta['filename']
