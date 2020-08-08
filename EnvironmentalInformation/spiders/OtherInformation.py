import datetime
import json
import logging
import re

import pandas
import scrapy
from scrapy import Request

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import OtherInformationRewardItem, OtherInformationItem, OtherInformationFileItem

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
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 100,
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
    url_filedown = "https://xxgk.eic.sh.cn:443/xhyf/common/filedown1.do?fileId={}"

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
        for wryCode, url_id in df[['污染源编码', 'url_id']].values.tolist():
            yield scrapy.FormRequest(url=self.url_otherInfo, formdata={"wryCode": wryCode},
                                     callback=self.parse_otherInfo_total)
            yield Request(self.url_ZXJC.format(url_id), callback=self.parse_ZXJC,
                          meta={"url_id": url_id, "title": "ZXJC"})
            yield Request(self.url_SHZR.format(url_id), callback=self.parse_ZXJC,
                          meta={"url_id": url_id, "title": "SHZR"})

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

    # 环保奖励情况
    def parse_otherInfo(self, response):
        if response.status == 200:
            data = json.loads(response.text)
            logger.info(f"环保奖励情况：{data}")
            item = OtherInformationRewardItem()
            item["dict_data"] = data.get("rows")
            yield item

    # 自行监测方案、社会责任报告共用
    def parse_ZXJC(self, response):
        th = response.xpath(r"//table/thead/tr/th/text()")
        td = response.xpath(r"//table/tbody/tr/td")
        for i in range(len(td) // len(th)):
            dict_data = dict()
            for j in range(len(th)):
                if td[i * 3 + j].xpath(r"a"):
                    data = re.findall(".*filedown\(\\'(.*)\\'.*", td[i * 3 + j].extract())
                    dict_data[th[j].get().strip()] = self.url_filedown.format(data[0])
                    dict_data["filename"] = data[0]
                else:
                    dict_data[th[j].get().strip()] = td[i * 3 + j].xpath(r"text()").get().strip()
            url_id = response.meta["url_id"]
            dict_data["url_id"] = url_id

            item = OtherInformationItem()
            item['dict_data'] = dict_data
            # 从请求的meta中获取请求的类型区分请求来源，提供pipeline做区分保存
            item["title"] = response.meta["title"]
            yield item

            # 文件下载
            fileItem = OtherInformationFileItem()
            fileItem['file_urls'] = [dict_data["操作"]]
            fileItem['files'] = [f"{dict_data['filename'][0]}.{dict_data['文件名称'].split('.')[-1]}"]
            yield fileItem

