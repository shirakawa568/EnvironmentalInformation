import datetime

import pandas
import scrapy
from scrapy import Request

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import EnterprisesDetailItem


class EnterprisesDetailSpider(scrapy.Spider):
    name = 'enterprises_detail'
    base_url = 'https://xxgk.eic.sh.cn/jsp/view/info.jsp?id={}'
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EnterprisesDetailPipeline': 300,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\EnterprisesDetail-{today}.log',
    }

    def start_requests(self):
        # count = 0
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="Sheet1", header=0)
        for uid, url_id in df[['id', 'url_id']].values.tolist():
            yield Request(self.base_url.format(url_id), meta={"url_id": url_id})

    def parse(self, response):
        th = response.xpath(r"//table/tr/th/text()")
        td = response.xpath(r"//table/tr/td/text()")
        info = zip(th, td)
        dict_detail = dict()
        for title, data in info:
            dict_detail[title.get().strip()] = data.get().strip()
        url_id = response.meta["url_id"]
        dict_detail["url_id"] = url_id

        item = EnterprisesDetailItem()
        item['dict_detail'] = dict_detail
        yield item
