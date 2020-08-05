import datetime
import json
import logging

import pandas
import scrapy
from scrapy import Request

from EnvironmentalInformation.common.tools import get_root_path

logger = logging.getLogger(__name__)

class OtherInformationSpider(scrapy.Spider):
    name = 'OtherInformation'
    # allowed_domains = ['xxgk.eic.sh.cn/jsp/view/otherInfo.jsp']
    # start_urls = ['http://xxgk.eic.sh.cn/jsp/view/otherInfo.jsp/']

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.OtherInformationFilePipeline': 100,
            'EnvironmentalInformation.pipelines.OtherInformationPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\OtherInformation-{today}.log',
        'FILES_STORE': f'{root_path}其他信息'
    }

    # 分页参数
    page_size = 50

    url_otherInfo = "https://xxgk.eic.sh.cn:443/jsp/view/other/list.do"
    url_ZXJC = "https://xxgk.eic.sh.cn/jsp/view/common/file_list2.jsp?ST_EVENT_ID={}&ST_FILE_CATEGORY=OTHER_ZXJC"
    url_SHZR = "https://xxgk.eic.sh.cn/jsp/view/common/file_list2.jsp?ST_EVENT_ID={}&ST_FILE_CATEGORY=OTHER_SHZR"

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode, url_id in df[['污染源编码', 'url_id']].values.tolist():
            yield scrapy.FormRequest(url=self.url_otherInfo, formdata={"wryCode": wryCode},
                                     callback=self.parse_otherInfo_total)
            yield Request(self.url_ZXJC.format(url_id), callback=self.parse_ZXJC)
            yield Request(self.url_SHZR.format(url_id), callback=self.parse_SHZR)

    def parse_otherInfo_total(self, response):
        if response.status == 200:
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("=")[1]
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=self.url_otherInfo,
                                             formdata={"order": "asc", "wryCode": wryCode,
                                                       "pageSize": str(self.page_size), "currentPage": str(i)},
                                             callback=self.parse_otherInfo)

    def parse_otherInfo(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            logger.info(f"环保奖励情况：{data}")
            item = PollutionControlFacilitiesItem()
            item["product"] = None
            item["pullication"] = None
            item["pullicationEmissions"] = data.get('rows')
            yield item

    def parse_ZXJC(self, response):
        pass

    def parse_SHZR(self, response):
        pass
