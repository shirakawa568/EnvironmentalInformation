import datetime
import re

import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import LicenseInformationItem


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
        'FILES_STORE': f'{root_path}许可信息公开',
        # 代理配置
        "DOWNLOADER_MIDDLEWARES": {
            'EnvironmentalInformation.middlewares.ProxyMiddleware': 100,
        }
    }

    # 分页参数
    page_size = 10

    # 许可信息公开 列表
    host = "http://permit.mee.gov.cn"
    base_url = "http://permit.mee.gov.cn/permitExt/syssb/xkgg/xkgg!licenseInformation.action"

    def start_requests(self):
        data = {
            "province": '310000000000',
            "city": '310100000000',
        }
        yield scrapy.FormRequest(url=self.base_url, formdata=data, callback=self.parse_license_information_total)

    # 获取总页数，下一步分页请求  re_first(r".*共(.*)页.*")
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

    # 获取到分页，下一步子页面请求 /html/body/div[4]/div[3]/div/table/tbody/tr[1]
    def parse_license_information_index(self, response):
        lenght = len(response.xpath(r"//table/tr"))
        th = response.xpath(r"//table/tr[@class='tbhead']/td/text()")
        for i in range(2, lenght + 1):
            data = dict()
            td = response.xpath(f"//table/tr[{i}]/td/text()")
            url = response.xpath(f"//table/tr[{i}]/td/a/@href")
            for j in range(len(th) - 1):
                try:
                    data[th[j].get()] = td[j].get()
                except:
                    print("1111")
            data[th[-1].get()] = self.host + url.get()
            print(url)
            yield scrapy.Request(url=self.host + url.get(), callback=self.parse_license_information_detail)
            item = LicenseInformationItem()
            item["dict_index"] = data
            yield item

    # 获取到子页面
    def parse_license_information_detail(self, response):
        # 顶部标题与地址、行业、地区、发证机关
        dict_text = dict()
        title = response.xpath(r'/html/body/div/table/tr[1]/td[1]/p/text()').get()
        dict_text["企业名称"] = title
        text = response.xpath(r'/html/body/div/table/tr[2]/td[1]/p/text()').get()
        data = re.match(r".*生产经营场所地址：(.*)行业类别：(.*)所在地区：(.*)发证机关：(.*).*", text, re.S)
        dict_text["生产经营场所地址"] = data.group(1).strip()
        dict_text["行业类别"] = data.group(2).strip()
        dict_text["所在地区"] = data.group(3).strip()
        dict_text["发证机关"] = data.group(4).strip()
        dict_text["排污许可证正本"] = self.host + response.xpath(r'//table/tr[2]/td[2]/a[1]/@href').get().strip()
        dict_text["排污许可证副本"] = self.host + response.xpath(r'//table/tr[2]/td[2]/a[2]/@href').get().strip()
        dict_text["许可证编号"] = response.xpath(r'//table[@class="tab0"]/tr[2]/td[1]/text()').get().strip()
        dict_text["业务类型"] = response.xpath(r'//table[@class="tab0"]/tr[2]/td[2]/text()').get().strip()
        dict_text["版本"] = response.xpath(r'//table[@class="tab0"]/tr[2]/td[3]/text()').get().strip()
        dict_text["办结日期"] = response.xpath(r'//table[@class="tab0"]/tr[2]/td[4]/text()').get().strip()
        dict_text["有效期限"] = response.xpath(r'//table[@class="tab0"]/tr[2]/td[5]/text()').get().strip()

        #
        dict_text["主要污染物类别"] = response.xpath(r'//table[@class="edit-grid"]/tr[1]/td[1]/text()').get().strip()
        dict_text["大气主要污染物种类"] = response.xpath(r'//table[@class="edit-grid"]/tr[2]/td[1]/text()').get().strip()
        dict_text["大气污染物排放规律"] = response.xpath(r'//table[@class="edit-grid"]/tr[3]/td[1]/text()').get().strip()
        dict_text["大气污染物排放执行标准"] = response.xpath(r'//table[@class="edit-grid"]/tr[4]/td[1]/text()').get().strip()
        dict_text["废水主要污染物种类"] = response.xpath(r'//table[@class="edit-grid"]/tr[5]/td[1]/text()').get().strip()
        dict_text["废水污染物排放规律"] = response.xpath(r'//table[@class="edit-grid"]/tr[6]/td[1]/text()').get().strip()
        dict_text["废水污染物排放执行标准"] = response.xpath(r'//table[@class="edit-grid"]/tr[7]/td[1]/text()').get().strip()
        dict_text["排污权使用和交易信息"] = response.xpath(r'//table[@class="edit-grid"]/tr[8]/td[1]/text()').get().strip()
        item_detail = LicenseInformationItem()
        item_detail["dict_detail"] = dict_text
        yield item_detail
        print(dict_text)
