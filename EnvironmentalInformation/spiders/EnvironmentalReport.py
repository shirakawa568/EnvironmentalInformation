import datetime
import json
import re

import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import FilesDownloadItem, EnvironmentalReportItem


class EnvironmentalReportSpider(scrapy.Spider):
    name = 'EnvironmentalReport'
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 100,
            'EnvironmentalInformation.pipelines.EnvironmentalReportPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\EnvironmentalReport-{today}.log',
        'FILES_STORE': f'{root_path}拟报批的环境影响报告表'
    }

    # 分页参数
    page_size = 20

    # 项目清单
    eiaReportList = "https://xxgk.eic.sh.cn/jsp/view/eiaReportList.jsp"
    eiaReportDetail = "https://xxgk.eic.sh.cn/jsxmxxgk/eiareport/action/jsxm_eiaReportDetail.do?from=jsxm&stEiaId={}&type={}"
    eiaReportDownload = "https://xxgk.eic.sh.cn/shhjkxw/eiareport/action/download.do?stEiaId={}&fileType={}"
    eiaBgsReportDownload = "https://xxgk.eic.sh.cn/shhjkxw/eiareportBgs/action/download.do?stEiaId={}&fileType={}"
    def start_requests(self):
        yield scrapy.Request(url=self.eiaReportList, callback=self.parse_environmental_report_total)

    def parse_environmental_report_total(self, response):
        page_num = response.xpath(r"/html/body/div[@id='wrapper']/div[@class='page']/span[3]/text()").re_first(
            r"共([0-9]*)页")
        page_num = int(page_num)
        for num in range(1, page_num + 1):
            data = {
                "currentPage": str(num)
            }
            yield scrapy.FormRequest(url=self.eiaReportList, formdata=data, callback=self.parse_project_id)

    def parse_project_id(self, response):
        data = response.xpath(r"//table/tr")
        for item in data:
            text = item.get()
            project_id = re.findall(".*openInfo\(\\'(.*)\\',.*", text)
            report_type = re.findall(".*openInfo\(\\'.*\\','(.*)\\'\).*", text)
            project_name = re.findall(r".*left\">(.*)</td.*", text)
            td = re.findall(r".*<td>(.*)</td>.*", text)
            if len(project_id) > 0:
                yield scrapy.Request(url=self.eiaReportDetail.format(project_id[0], report_type[0]),
                                     callback=self.parse_detail,
                                     meta={"project_id": project_id[0],
                                           "project_name": project_name[0],
                                           "audit_department": td[2],
                                           "address": project_name[1],
                                           "release_time": td[5],
                                           "closing_date": td[6],
                                           "report_type": report_type[0],
                                           })

    def parse_detail(self, response):
        data = response.xpath(r'//*[@id="wrapper"]/div[3]/div/div/div[3]/ul/li[6]')
        if len(data) == 0:
            print("详情页下载地址获取失败,更换方案")
            data = ["BL_GSWTS"]
        else:
            data = re.findall(".*filedown\('(.*)'.*", data[0].get())
        fileItem = FilesDownloadItem()
        # 判断报告表 报告书
        if response.meta["report_type"] == "报告书":
            fileItem["file_urls"] = [self.eiaBgsReportDownload.format(response.meta["project_id"], data[0])]
        else:
            fileItem["file_urls"] = [self.eiaReportDownload.format(response.meta["project_id"], data[0])]
        fileItem["files"] = [f"{response.meta['project_id']}.pdf"]
        yield fileItem

        item = EnvironmentalReportItem()
        item["dict_data"] = {
            "project_name": response.meta["project_name"],
            "project_id": response.meta["project_id"],
            "audit_department": response.meta["audit_department"],
            "address": response.meta["address"],
            "release_time": response.meta["release_time"],
            "closing_date": response.meta["closing_date"],
            "project_url": self.eiaReportDownload.format(response.meta["project_id"], data[0])
        }
        yield item
