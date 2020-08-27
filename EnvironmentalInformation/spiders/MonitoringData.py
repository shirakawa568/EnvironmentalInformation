import datetime
import json

import pandas
import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import MonitoringDataItem


class MonitoringDataSpider(scrapy.Spider):
    name = 'MonitoringData'

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.MonitoringDataPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\MonitoringData-{today}.log',
    }

    # 分页参数
    page_size = 8

    dict_url = {
        "waterHand": "https://xxgk.eic.sh.cn/jsp/view/waterHand/list.do",
        "wasteHand": "https://xxgk.eic.sh.cn/jsp/view/wasteHand/list.do",
        "noiseHand": "https://xxgk.eic.sh.cn/jsp/view/noiseHand/list.do",
        "waterAuto": "https://xxgk.eic.sh.cn/jsp/view/waterAuto/list.do",
        "wasteAuto": "https://xxgk.eic.sh.cn/jsp/view/wasteAuto/list.do",
    }

    now_time = datetime.datetime.now()
    month = now_time.month
    future_mouth_first = datetime.datetime(now_time.year, month + 1, 1, 23, 59, 59)
    # startDate = datetime.datetime(now_time.year, month, 1, 0, 0, 0)
    # endDate = future_mouth_first - datetime.timedelta(days=1)
    startDate = now_time + datetime.timedelta(days=-1)
    endDate = now_time

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode, url_id in df[['污染源编码', 'url_id']].values.tolist():
            for title, url in self.dict_url.items():
                yield scrapy.FormRequest(url=url, formdata={"order": "asc",
                                                            "pscode": wryCode,
                                                            "wryCode": wryCode,
                                                            "pageSize": "10",
                                                            "currentPage": "1",
                                                            "beginDate": self.startDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                            "endDate": self.endDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                            },
                                         callback=self.parse_monitoring_data_total,
                                         meta={"url_id": url_id, "title": title, "wryCode": wryCode})

    def parse_monitoring_data_total(self, response):
        if response.status == 200 and response.text != '':
            total = json.loads(response.text).get("total")
            wryCode = response.request.body.decode().split("&")[1].split("=")[1]
            if total > 0:
                for i in range(1, total // self.page_size + 2):
                    yield scrapy.FormRequest(url=response.request.url,
                                             formdata={"order": "asc",
                                                       "pscode": wryCode,
                                                       "wryCode": wryCode,
                                                       "pageSize": str(self.page_size),
                                                       "currentPage": str(i),
                                                       "beginDate": self.startDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                       "endDate": self.endDate.strftime('%Y-%m-%d %H:%M:%S'),
                                                       },
                                             callback=self.parse,
                                             meta=response.meta)

            if response.meta["url_id"] == "1017":
                print()

    def parse(self, response):
        if response.meta["url_id"] == "1017":
            print()
        if response.status == 200:
            data = json.loads(response.text)
            item = MonitoringDataItem()
            item["dict_data"] = data.get("rows")
            item["title"] = response.meta["title"]
            item["url_id"] = response.meta["url_id"]
            item["wryCode"] = response.meta["wryCode"]
            yield item
