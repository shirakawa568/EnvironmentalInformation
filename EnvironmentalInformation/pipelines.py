# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pandas

from EnvironmentalInformation.spiders.enterprises_detail import EnterprisesDetailSpider
from EnvironmentalInformation.spiders.enterprises_info import EnterprisesInfoSpider
from common.tools import get_root_path, add_sheet


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
