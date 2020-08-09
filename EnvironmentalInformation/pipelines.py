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

from EnvironmentalInformation.common.tools import get_root_path, add_sheet
from EnvironmentalInformation.items import OtherInformationRewardItem, EnvironmentalReportItem, \
    InformationDisclosureItem, InformationDisclosureDetailItem, CleanerProductionItem
from EnvironmentalInformation.spiders.enterprises_detail import EnterprisesDetailSpider
from EnvironmentalInformation.spiders.enterprises_info import EnterprisesInfoSpider

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
            print(f"{self.item_count}\t{self.counts}\t收到{len(item['pullicationEmissions'])}条污染物排放方式及排放去向")
            return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        logger.info("爬取内容条数：", self.counts, "激活Item数：", self.item_count)
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
        logger.info("爬取内容条数：", self.counts, "Item总数：", self.item_count)
        # 覆盖保存
        self.df_PWXK.to_excel(self.writer, sheet_name='排污许可证', index=False)
        self.writer.save()
        # 追加保存
        if self.df_JSXM is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, "建设项目行政办理事项",
                      self.df_JSXM)
        # 追加保存
        if self.df_WFJY is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, "Sheet3",
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


#
class OtherInformationPipeline:
    filename = "OtherInformation.xlsx"

    def __init__(self):
        self.df_reward = None
        self.df_ZXJC = None
        self.df_SHZR = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)
        self.counts = [0, 0, 0]
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        # 环保奖励情况
        if isinstance(item, OtherInformationRewardItem):
            if self.df_reward is None:
                cols: dict = item['dict_data'][0].keys()
                self.df_reward = pandas.DataFrame(columns=cols)
            for col in item['dict_data']:
                s = pandas.Series(col)
                self.df_reward = self.df_reward.append(s, ignore_index=True)
            self.counts[0] += len(item['dict_data'])
            print(f"{self.item_count}\t{self.counts}\t收到{len(item['dict_data'])}条环保奖励情况")
            return item
        # 自行监测方案
        if item.get('title') == 'ZXJC':
            if self.df_ZXJC is None:
                cols: dict = item['dict_data'].keys()
                self.df_ZXJC = pandas.DataFrame(columns=cols)
            s = pandas.Series(item['dict_data'])
            self.df_ZXJC = self.df_ZXJC.append(s, ignore_index=True)
            self.counts[1] += 1
            print(f"{self.item_count}\t{self.counts}\t收到1条自行监测方案")
            return item
        # 社会责任报告
        if item.get('title') == 'SHZR':
            if self.df_SHZR is None:
                cols: dict = item['dict_data'].keys()
                self.df_SHZR = pandas.DataFrame(columns=cols)
            s = pandas.Series(item['dict_data'])
            self.df_SHZR = self.df_SHZR.append(s, ignore_index=True)
            self.counts[2] += 1
            print(f"{self.item_count}\t{self.counts}\t收到1条社会责任报告")
            return item
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        logger.info("爬取内容条数：", self.counts, "Item总数：", self.item_count)
        # 覆盖保存
        self.df_reward.to_excel(self.writer, sheet_name='环保奖励情况', index=False)
        self.writer.save()
        # 追加保存:自行监测方案
        if self.df_ZXJC is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="自行监测方案",
                      dataframe=self.df_ZXJC)
        # 追加保存:社会责任报告
        if self.df_SHZR is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="社会责任报告",
                      dataframe=self.df_SHZR)
        # 爬虫结束
        logger.info("save complete")


class MonitoringDataPipeline:
    filename = "MonitoringData.xlsx"

    def __init__(self):
        self.df_waterHand = None
        self.df = {
            "waterHand": None, "wasteHand": None, "noiseHand": None, "waterAuto": None, "wasteAuto": None
        }
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)
        self.counts = dict()
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        if self.df[item["title"]] is None:
            cols: dict = item['dict_data'][0].keys()
            self.df[item["title"]] = pandas.DataFrame(columns=cols)
        for col in item['dict_data']:
            s = pandas.Series(col)
            self.df[item["title"]] = self.df[item["title"]].append(s, ignore_index=True)
            if self.counts.get(item["title"]) is None:
                self.counts[item["title"]] = 1
            else:
                self.counts[item["title"]] += 1
        print(self.counts)
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        logger.info(f"爬取内容条数：{self.counts}")
        logger.info(f"Item总数：{self.item_count}")
        # 覆盖保存:废水手工监测记录
        self.df["waterHand"].to_excel(self.writer, sheet_name='废水手工监测记录', index=False)
        self.writer.save()
        # 追加保存:废气手工监测记录
        if self.df["wasteHand"] is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="废气手工监测记录",
                      dataframe=self.df["wasteHand"])
        # 噪声手工监测记录
        if self.df["noiseHand"] is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="噪声手工监测记录",
                      dataframe=self.df["noiseHand"])
        # 废水在线监测记录
        if self.df["waterAuto"] is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="废水在线监测记录",
                      dataframe=self.df["waterAuto"])
        # 废气在线监测记录
        if self.df["wasteAuto"] is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="废气在线监测记录",
                      dataframe=self.df["wasteAuto"])
        # 爬虫结束
        logger.info("save complete")


class ManualMonitoringPipeline:
    filename = "ManualMonitoring.xlsx"

    def __init__(self):
        self.df = {
            "sgWater": None, "sgWaste": None
        }
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)
        self.counts = dict()
        self.item_count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        self.item_count += 1
        if self.df[item["title"]] is None:
            cols: dict = item['dict_data'][0].keys()
            self.df[item["title"]] = pandas.DataFrame(columns=cols)
        for col in item['dict_data']:
            s = pandas.Series(col)
            self.df[item["title"]] = self.df[item["title"]].append(s, ignore_index=True)
            if self.counts.get(item["title"]) is None:
                self.counts[item["title"]] = 1
            else:
                self.counts[item["title"]] += 1
        print(self.counts)
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        logger.info(f"爬取内容条数：{self.counts}")
        logger.info(f"Item总数：{self.item_count}")
        # 覆盖保存:废水手工监测记录
        if self.df["sgWater"] is not None:
            self.df["sgWater"].to_excel(self.writer, sheet_name='废水手工监测记录', index=False)
            self.writer.save()
        # 追加保存:废气手工监测记录
        if self.df["sgWaste"] is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="废气手工监测记录",
                      dataframe=self.df["wasteHand"])
        # 爬虫结束
        logger.info("save complete")


# 拟报批的环境影响报告表
class EnvironmentalReportPipeline:
    filename = "EnvironmentalReport.xlsx"

    def __init__(self):
        self.df = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)
        self.count = 0

    def open_spider(self, spider):
        logger.info('spider will starting')

    def process_item(self, item, spider):
        if isinstance(item, EnvironmentalReportItem):
            self.count += 1
            if self.df is None:
                titles: dict = item["dict_data"].keys()
                self.df = pandas.DataFrame(columns=titles)
            s = pandas.Series(item['dict_data'])
            self.df = self.df.append(s, ignore_index=True)
            print(self.count)
            return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        logger.info(f"Item总数：{self.count}")
        # 覆盖保存:拟报批的环境影响报告表
        if self.df is not None:
            self.df.to_excel(self.writer, sheet_name='拟报批的环境影响报告表', index=False)
            self.writer.save()
        # 爬虫结束
        logger.info("save complete")


class InformationDisclosurePipeline:
    filename = "InformationDisclosure.xlsx"

    def __init__(self):
        self.df = pandas.DataFrame(columns=['id', 'project_name', 'area', 'current_stage', 'public_date', 'project_id'])
        self.df_detail = None
        self.df_files = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)

    def open_spider(self, spider):
        print("环评事中事后信息公开爬虫开始")

    def process_item(self, item, spider):
        if isinstance(item, InformationDisclosureItem):
            s = pandas.Series(item)
            self.df = self.df.append(s, ignore_index=True)
        if isinstance(item, InformationDisclosureDetailItem):
            # 处理详情列表
            if self.df_detail is None:
                titles = item["dict_data"].keys()
                self.df_detail = pandas.DataFrame(columns=titles)
            s = pandas.Series(item["dict_data"])
            self.df_detail = self.df_detail.append(s, ignore_index=True)
            # 处理文件列表
            if self.df_files is None:
                titles = item["list_files"][0].keys()
                self.df_files = pandas.DataFrame(columns=titles)
            for row in item["list_files"]:
                s = pandas.Series(row)
                self.df_files = self.df_files.append(s, ignore_index=True)
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        self.df = self.df.sort_values('id', axis=0, ascending=True)
        # 保存Excel
        root_path = get_root_path('EnvironmentalInformation')
        self.df.to_excel(self.writer, sheet_name="环评事中事后信息公开", index=False)
        self.writer.save()
        # 追加sheet项目详情
        if self.df_detail is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="项目详情",
                      dataframe=self.df_detail)
        # 追加sheet文件关联表
        if self.df_files is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="文件列表",
                      dataframe=self.df_files)
        logger.info("save complete")


# 清洁生产
class CleanerProductionPipeline:
    filename = "CleanerProduction.xlsx"

    def __init__(self):
        self.df_crop_info = None
        self.df_double_super = None
        self.df_use_materials = None
        self.df_discharge_materials = None
        self.df_generation_disposal = None
        self.writer = pandas.ExcelWriter(get_root_path('EnvironmentalInformation') + self.filename)

    def open_spider(self, spider):
        print("清洁生产爬虫开始")

    def process_item(self, item, spider):
        if isinstance(item, CleanerProductionItem):
            # 公司信息
            if self.df_crop_info is None:
                titles = item["crop_info"].keys()
                self.df_crop_info = pandas.DataFrame(columns=titles)
            s = pandas.Series(item["crop_info"])
            self.df_crop_info = self.df_crop_info.append(s, ignore_index=True)
            # 双超信息
            if len(item["double_super"]) > 0:
                if self.df_double_super is None:
                    titles = item["double_super"][0].keys()
                    self.df_double_super = pandas.DataFrame(columns=titles)
                for row in item["double_super"]:
                    s = pandas.Series(row)
                    self.df_double_super = self.df_double_super.append(s, ignore_index=True)
            # 双有信息 - 使用有毒有害原料
            if len(item["use_materials"]) > 0:
                if self.df_use_materials is None:
                    titles = item["use_materials"][0].keys()
                    self.df_use_materials = pandas.DataFrame(columns=titles)
                for row in item["use_materials"]:
                    s = pandas.Series(row)
                    self.df_use_materials = self.df_use_materials.append(s, ignore_index=True)
            # 双有信息 - 排放有毒有害物质
            if len(item["discharge_materials"]) > 0:
                if self.df_discharge_materials is None:
                    titles = item["discharge_materials"][0].keys()
                    self.df_discharge_materials = pandas.DataFrame(columns=titles)
                for row in item["discharge_materials"]:
                    s = pandas.Series(row)
                    self.df_discharge_materials = self.df_discharge_materials.append(s, ignore_index=True)
            # 双有信息 - 危险废物的产生和处置
            if len(item["generation_disposal"]) > 0:
                if self.df_generation_disposal is None:
                    titles = item["generation_disposal"][0].keys()
                    self.df_generation_disposal = pandas.DataFrame(columns=titles)
                for row in item["generation_disposal"]:
                    s = pandas.Series(row)
                    self.df_generation_disposal = self.df_generation_disposal.append(s, ignore_index=True)
        return item

    def close_spider(self, spider):
        logger.info('spider is ending')
        # 保存Excel
        self.df_crop_info.to_excel(self.writer, sheet_name="公司信息", index=False)
        self.writer.save()
        # 追加sheet双超信息
        if self.df_double_super is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="双超信息",
                      dataframe=self.df_double_super)
        # 追加sheet双有信息 - 使用有毒有害原料
        if self.df_use_materials is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name="双有信息 - 使用有毒有害原料",
                      dataframe=self.df_use_materials)
        # 追加sheet # 双有信息 - 排放有毒有害物质
        if self.df_discharge_materials is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name=" # 双有信息 - 排放有毒有害物质",
                      dataframe=self.df_discharge_materials)
        # 追加sheet # 双有信息 - 危险废物的产生和处置
        if self.df_generation_disposal is not None:
            add_sheet(get_root_path('EnvironmentalInformation'), self.filename, sheet_name=" # 双有信息 - 危险废物的产生和处置",
                      dataframe=self.df_generation_disposal)
        logger.info("save complete")


class LicenseInformationPipeline:
    pass
