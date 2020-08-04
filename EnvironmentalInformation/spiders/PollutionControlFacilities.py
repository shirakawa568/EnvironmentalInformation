import datetime
import json
import logging

import pandas
import scrapy

from EnvironmentalInformation.items import PollutionControlFacilitiesItem
from common.tools import get_root_path

logger = logging.getLogger(__name__)


class PollutionControlFacilitiesSpider(scrapy.Spider):
    name = 'PollutionControlFacilities'
    # allowed_domains = ['https://xxgk.eic.sh.cn/']
    # start_urls = ['http://https://xxgk.eic.sh.cn//']
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 产生污染设施情况
    product = "https://xxgk.eic.sh.cn/jsp/view/product/list.do"
    # 污染处理设施建设运行情况
    pullication = "https://xxgk.eic.sh.cn/jsp/view/pullication/list.do"
    # 污染物排放方式及排放去向
    pullicationEmissions = "https://xxgk.eic.sh.cn/jsp/view/pullicationEmissions/list.do"

    # 分页参数
    page_size = 50

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.PollutionControlFacilitiesPipeline': 200,
        },
        'LOG_FILE': f'{root_path}log\\PollutionControlFacilities-{today}.log',
    }

    def start_requests(self):
        # 获取企事业单位urlId
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode in df['污染源编码'].values.tolist():
            yield scrapy.FormRequest(url=self.product, formdata={"wryCode": wryCode},
                                     callback=self.parse_product_total)
            yield scrapy.FormRequest(url=self.pullication, formdata={"wryCode": wryCode},
                                     callback=self.parse_pullication_total)
            yield scrapy.FormRequest(url=self.pullicationEmissions, formdata={"wryCode": wryCode},
                                     callback=self.parse_pullicationEmissions_total)

        print("start_requests end")

    # 根据产生污染设施情况条目总数纵向爬取所有页
    def parse_product_total(self, response):
        if response.status == 200:
            # self.get_total_and_continue(response, self.product, "产生污染设施情况", self.parse_product)
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("=")[1]
            logger.info(f"wryCode:{wryCode}包含{total}条 产生污染设施情况")
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=self.product,
                                             formdata={"order": "asc", "wryCode": wryCode,
                                                       "pageSize": str(self.page_size), "currentPage": str(i)},
                                             callback=self.parse_product)

    # 根据 污染处理设施建设运行情况 条目总数纵向爬取所有页
    def parse_pullication_total(self, response):
        if response.status == 200:
            # self.get_total_and_continue(response, self.pullication, "污染处理设施建设运行情况", self.parse_pullication)
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("=")[1]
            logger.info(f"wryCode:{wryCode}包含{total}条 污染处理设施建设运行情况")
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=self.pullication,
                                             formdata={"order": "asc", "wryCode": wryCode,
                                                       "pageSize": str(self.page_size), "currentPage": str(i)},
                                             callback=self.parse_pullication)

    def parse_pullicationEmissions_total(self, response):
        if response.status == 200:
            # self.get_total_and_continue(response, self.pullicationEmissions, "污染物排放方式及排放去向",
            #                             self.parse_pullicationEmissions)
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("=")[1]
            logger.info(f"wryCode:{wryCode}包含{total}条 污染物排放方式及排放去向")
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=self.pullicationEmissions,
                                             formdata={"order": "asc", "wryCode": wryCode,
                                                       "pageSize": str(self.page_size), "currentPage": str(i)},
                                             callback=self.parse_pullicationEmissions)

    def get_total_and_continue(self, response, url, title, parse):
        total = json.loads(response.text).get("total")
        wryCode = response.request.body.decode().split("=")[1]
        logger.info(f"wryCode:{wryCode}包含{total}条 {title}")
        if total > 0:
            for i in range(1, total // self.page_size + 2):
                yield scrapy.FormRequest(url=url,
                                         formdata={"order": "asc", "wryCode": wryCode,
                                                   "pageSize": str(self.page_size), "currentPage": str(i)},
                                         callback=parse)

    def parse_product(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            logger.info(f"收到响应：{data}")
            if len(data) > 0:
                item = PollutionControlFacilitiesItem()
                item["product"] = data.get('rows')
                item["pullication"] = None
                item["pullicationEmissions"] = None
                yield item
            else:
                wryCode = response.request.body.decode().split("&")[1].split("=")[1]
                logger.info(f"wryCode:{wryCode},收到响应：数据为空")
        else:
            logger.error("响应异常")

    def parse_pullication(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            logger.info(f"收到响应：{data}")
            if len(data) > 0:
                item = PollutionControlFacilitiesItem()
                item["product"] = None
                item["pullication"] = data.get('rows')
                item["pullicationEmissions"] = None
                yield item
            else:
                wryCode = response.request.body.decode().split("&")[1].split("=")[1]
                logger.info(f"wryCode:{wryCode},收到响应：数据为空")
        else:
            logger.error("响应异常")

    def parse_pullicationEmissions(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            logger.info(f"收到响应：{data}")
            if len(data) > 0:
                item = PollutionControlFacilitiesItem()
                item["product"] = None
                item["pullication"] = None
                item["pullicationEmissions"] = data.get('rows')
                yield item
            else:
                wryCode = response.request.body.decode().split("&")[1].split("=")[1]
                logger.info(f"wryCode:{wryCode},收到响应：数据为空")
        else:
            logger.error("响应异常")
