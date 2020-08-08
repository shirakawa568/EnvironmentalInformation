import datetime
import re

import pandas
import scrapy
from scrapy import Request

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import EmergencyPlanItem


class EmergencyPlanSpider(scrapy.Spider):
    name = 'EmergencyPlan'
    # allowed_domains = ['xxgk.eic.sh.cn']
    base_url = "https://xxgk.eic.sh.cn/jsp/view/common/file_list2.jsp?ST_EVENT_ID={}&ST_FILE_CATEGORY=YJYA_YAWB"
    file_url = "https://xxgk.eic.sh.cn:443/xhyf/common/filedown1.do?fileId={}"

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 1,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\EmergencyPlan-{today}.log',
        'FILES_STORE': f'{root_path}应急预案'
    }

    def start_requests(self):
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="Sheet1", header=0)
        for url_id in df['url_id'].values.tolist():
            yield Request(self.base_url.format(url_id))

    def parse(self, response):
        file = response.xpath(r'//*[@id="fileList"]/tbody/tr/td[1]/text()').extract()
        url = response.xpath(r'//*[@id="fileList"]/tbody/tr/td[3]').extract()
        urls, files = list(), list()
        for i in range(len(url)):
            p = re.findall(".*filedown\(\\'(.*)\\'.*", url[i])
            urls.append(self.file_url.format(p[0]))
            files.append(p[0] + "." + file[i].split(".")[-1])

        item = EmergencyPlanItem()
        item['file_urls'] = urls
        item['files'] = files
        yield item
