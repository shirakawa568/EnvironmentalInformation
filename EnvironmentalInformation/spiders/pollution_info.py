import datetime
import json
import logging

import pandas
import scrapy
from scrapy import Request

from EnvironmentalInformation.items import PollutionInfoItem, EnterprisesImageItem
from common.tools import get_root_path

logger = logging.getLogger(__name__)


class PollutionInfoSpider(scrapy.Spider):
    """
    该爬虫抓取企事业单位详细信息页的排污信息标签页
    通过3段 yield 向scrapy引擎提交该页面3块信息
    """
    name = 'pollution_info'
    # allowed_domains = ['https://xxgk.eic.sh.cn/']
    # start_urls = ['http://https://xxgk.eic.sh.cn//']
    url_port = 'https://xxgk.eic.sh.cn/jsp/view/port/list.do'
    url_jcdw = 'https://xxgk.eic.sh.cn/jsp/view/jcdw/list.do'
    url_productinfo = "https://xxgk.eic.sh.cn/tbBase/productinfo/getTWryZlinfoList.do?date={}&ST_WRY_CODE={}"
    url_image = "https://xxgk.eic.sh.cn/jsp/view/jcdwjxm_list.jsp?baseId={}"

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    date = datetime.datetime.now().strftime("%Y-%m")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            # 'scrapy.pipelines.images.ImagesPipeline': 1,
            'EnvironmentalInformation.pipelines.PollutionInfoPipeline': 200,
            'EnvironmentalInformation.pipelines.ImgsPipeline': 300,
        },
        # 设置log日志
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\PollutionInfo-{today}.log',
        # 'LOG_STDOUT': True,
        # 文件存储
        'FILES_STORE': f'{root_path}厂区图',
    }

    def start_requests(self):
        # 获取企事业单位urlId
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode in df['污染源编码'].values.tolist():
            # 排放口编号
            yield scrapy.FormRequest(url=self.url_port,
                                     formdata={
                                         "order": "asc",
                                         "wryCode": wryCode,
                                     },
                                     callback=self.parse_pfk)
            # 检测项目
            yield scrapy.FormRequest(url=self.url_jcdw,
                                     formdata={
                                         "order": "asc",
                                         "wryCode": wryCode,
                                     },
                                     callback=self.get_total)
            # 实际排放总量
            yield Request(url=self.url_productinfo.format(self.date, wryCode), callback=self.parse_pfzl)
        # 厂区图
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="Sheet1", header=0)
        for url_id in df['url_id'].values.tolist():
            yield Request(self.url_image.format(url_id), callback=self.parse_img)

    # 排放口编号
    def parse_pfk(self, response):
        if response.status == 200:
            # 相应正常
            dict_result = json.loads(response.text)
            if len(dict_result.get('rows')) > 0:
                item = PollutionInfoItem()
                item["dict_pfk"] = dict_result.get('rows')
                item["dict_poll_project"] = None
                item['dict_pfzl'] = None
                item["images"] = None
                yield item
        else:
            logger.error("响应异常")

    # 获取条目总数，循环抓取每一页数据，每页20条
    def get_total(self, response):
        page_size = 5
        if response.status == 200:
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("&")[1].split("=")[1]
            logger.info(f"wryCode:{wryCode}包含{total}条排污项目")
            if total > 0:
                for i in range(1, total // page_size + 2):
                    yield scrapy.FormRequest(url=self.url_jcdw,
                                             formdata={
                                                 "order": "asc",
                                                 "wryCode": wryCode,
                                                 "pageSize": str(page_size),
                                                 "currentPage": str(i)
                                             },
                                             callback=self.parse_poll_project)

    # 检测项目
    def parse_poll_project(self, response):
        if response.status == 200:
            dict_result = json.loads(response.text)
            # logger.info(dict_result)
            # print(dict_result)
            if len(dict_result.get('rows')) > 0:
                item = PollutionInfoItem()
                item["dict_pfk"] = None
                item["dict_poll_project"] = dict_result.get('rows')
                item['dict_pfzl'] = None
                item["images"] = None
                yield item
        else:
            logger.error("响应异常")

    # 实际排放总量
    def parse_pfzl(self, response):
        if response.status == 200:
            th = response.xpath(r"//table/thead/tr/th/text()")
            td = response.xpath(r"//table/tbody/tr/td/text()")
            if len(td) > 0:
                info = zip(th, td)
                dict_pfzl = dict()
                for title, data in info:
                    dict_pfzl[title.get().strip()] = data.get().strip()

                item = PollutionInfoItem()
                item["dict_pfk"] = None
                item["dict_poll_project"] = None
                item['dict_pfzl'] = dict_pfzl
                item["images"] = None
                yield item

    def parse_img(self, response):
        if response.status == 200:
            img_url = response.xpath(r"//span[@id='cqtImg_a']/@href")
            if len(img_url) > 0:
                url = response.request.url.split('=')[1]
                item = PollutionInfoItem()
                item["dict_pfk"] = None
                item["dict_poll_project"] = None
                item['dict_pfzl'] = None
                item["images"] = img_url[0].root
                yield item
