import datetime

import scrapy

from EnvironmentalInformation.common.tools import get_root_path


class LicenseInformationSpider(scrapy.Spider):
    name = 'LicenseInformation'
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    year = datetime.datetime.now().year
    lastYear = year - 1

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 100,
            'EnvironmentalInformation.pipelines.LicenseInformationPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\LicenseInformation-{today}.log',
        'FILES_STORE': f'{root_path}许可信息公开'
    }

    # 分页参数
    page_size = 20

    # 许可信息公开 列表
    base_url = "http://permit.mee.gov.cn/permitExt/syssb/xkgg/xkgg!licenseInformation.action"

    def start_requests(self):
        data = {
            "province": '310000000000',
            "city": '310100000000',
        }
        yield scrapy.FormRequest(url=self.base_url, formdata=data, callback=self.parse_license_information_total)

    # 获取总页数，处理分页  re_first(r".*共(.*)页.*")
    def parse_license_information_total(self, response):
        page_num = response.xpath(r"//div[@class='fr margin-t-33 margin-b-20']/text()").re_first(r"共([0-9]*)页")
        page_num = int(page_num)
        for num in range(1, page_num + 1):
            data = {
                "page.pageNo": str(num),
                "province": '310000000000',
                "city": '310100000000',
            }
            yield scrapy.FormRequest(url=self.base_url, formdata=data,
                                     callback=self.parse_license_information_index)

    def parse_license_information_index(self, response):
        pass
