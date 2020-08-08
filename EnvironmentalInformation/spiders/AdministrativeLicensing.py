"""
行政许可

"""
import datetime
import json
import logging

import pandas
import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import AdministrativeLicensingItem

logger = logging.getLogger(__name__)


class AdministrativeLicensingSpider(scrapy.Spider):
    name = 'AdministrativeLicensing'
    # allowed_domains = ['https://xxgk.eic.sh.cn/jsp/view/xzxk/list.do']
    # start_urls = ['https://xxgk.eic.sh.cn/jsp/view/xzxk/list.do/']
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 分页参数
    page_size = 50

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.AdministrativeLicensingPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\AdministrativeLicensing-{today}.log',
    }
    # 行政许可
    base_url = "https://xxgk.eic.sh.cn/jsp/view/xzxk/list.do"
    # 类型:排污许可、建设项目、???、其它许可
    types = ["PWXK", "JSXM", "WFJY", "OTHER"]

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode in df['污染源编码'].values.tolist():
            for t in self.types:
                yield scrapy.FormRequest(url=self.base_url, formdata={"wryCode": wryCode, "type": t},
                                         callback=self.parse_total)

    def parse_total(self, response):
        if response.status == 200:
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("&")[0].split("=")[1]
            _type = response.request.body.decode().split("&")[1].split("=")[1]
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=self.base_url,
                                             formdata={"order": "asc", "wryCode": wryCode, "type": _type,
                                                       "pageSize": str(self.page_size), "currentPage": str(i)},
                                             callback=self.parse)

    def parse(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            wryCode = response.request.body.decode().split("&")[1].split("=")[1]
            _type = response.request.body.decode().split("&")[2].split("=")[1]
            if len(data) > 0:
                item = AdministrativeLicensingItem()
                item["wryCode"] = wryCode
                item["_type"] = _type
                item["data"] = data.get("rows")
                yield item
            else:
                logger.info(f"wryCode:{wryCode},收到响应：数据为空")
        else:
            logger.error(f"响应异常：{response.status}")
