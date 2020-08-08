import datetime
import json

import pandas
import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import ManualMonitoringItem


class ManualMonitoringSpider(scrapy.Spider):
    name = 'ManualMonitoring'

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.ManualMonitoringPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\ManualMonitoring-{today}.log',
    }

    # 分页参数
    page_size = 50

    dict_url = {
        "sgWater": "https://xxgk.eic.sh.cn/jsp/view/sgWater/list.do",
        "sgWaste": "https://xxgk.eic.sh.cn/jsp/view/sgWaste/list.do",
    }

    now_time = datetime.datetime.now()
    month = now_time.month
    future_mouth_first = datetime.datetime(now_time.year, month + 1, 1, 23, 59, 59)
    startDate = datetime.datetime(now_time.year, month - 2, 1, 0, 0, 0)
    endDate = future_mouth_first - datetime.timedelta(days=1)

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode, url_id in df[['污染源编码', 'url_id']].values.tolist():
            for title, url in self.dict_url.items():
                yield scrapy.FormRequest(url=url, formdata={"order": "asc",
                                                            "wryCode": wryCode,
                                                            "pageSize": "10",
                                                            "currentPage": "1",
                                                            "beginDate": self.startDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                            "endDate": self.endDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                            },
                                         callback=self.parse_manual_monitoring_total,
                                         meta={"url_id": url_id, "title": title})

    def parse_manual_monitoring_total(self, response):
        if response.status == 200 and response.text != '':
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("&")[1].split("=")[1]
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=response.request.url,
                                             formdata={"order": "asc",
                                                       "wryCode": wryCode,
                                                       "pageSize": str(self.page_size),
                                                       "currentPage": str(i),
                                                       "beginDate": self.startDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                       "endDate": self.endDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                       },
                                             callback=self.parse,
                                             meta=response.meta)

    def parse(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            item = ManualMonitoringItem()
            item["dict_data"] = data.get("rows")
            item["title"] = response.meta["title"]
            item["url_id"] = response.meta["url_id"]
            yield item
