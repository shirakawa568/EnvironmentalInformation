import datetime
import json
import logging

import pandas
import scrapy

from EnvironmentalInformation.items import PollutionInfoItem
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

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.PollutionInfoPipeline': 300,
        },
        # 设置log日志
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\PollutionInfo-{today}.log',
        # 'LOG_STDOUT': True,
    }

    def start_requests(self):
        # 获取企事业单位urlId
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode in df['污染源编码'].values.tolist():
            yield scrapy.FormRequest(url=self.url_port,
                                     formdata={
                                         "order": "asc",
                                         "wryCode": wryCode,
                                     },
                                     callback=self.parse_pfk)

            yield scrapy.FormRequest(url=self.url_jcdw,
                                     formdata={
                                         "order": "asc",
                                         "wryCode": wryCode,
                                     },
                                     callback=self.get_total)

    def parse_pfk(self, response):
        if response.status == 200:
            # 相应正常
            dict_result = json.loads(response.text)
            if len(dict_result.get('rows')) > 0:
                item = PollutionInfoItem()
                item["dict_pfk"] = dict_result.get('rows')
                item["dict_poll_project"] = None
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

    def parse_poll_project(self, response):
        if response.status == 200:
            dict_result = json.loads(response.text)
            # logger.info(dict_result)
            # print(dict_result)
            if len(dict_result.get('rows')) > 0:
                item = PollutionInfoItem()
                item["dict_pfk"] = None
                item["dict_poll_project"] = dict_result.get('rows')
                item["images"] = None
                yield item
        else:
            logger.error("响应异常")
