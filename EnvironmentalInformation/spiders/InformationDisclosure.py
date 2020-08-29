"""
环评事中事后信息公开

修改为更具已有的企事业单位的名称查询项目，获取项目的详情页，过滤掉无用的项目数据，增加爬虫的效率

"""
import datetime
import json

import pandas
import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import InformationDisclosureItem, InformationDisclosureDetailItem, FilesDownloadItem


class InformationDisclosureSpider(scrapy.Spider):
    name = 'InformationDisclosure'
    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 100,
            'EnvironmentalInformation.pipelines.InformationDisclosurePipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\InformationDisclosure-{today}.log',
        'FILES_STORE': f'{root_path}环评事中事后信息公开'
    }

    # 分页参数
    page_size = 20

    # 环评事中事后信息公开 列表
    jsxmxxgkInfo_main = "https://xxgk.eic.sh.cn/jsp/view/jsxmxxgkInfo_main.jsp"
    subPage = "https://xxgk.eic.sh.cn/jsp/view/jsxmInfo_edit.jsp?from=jsxm&id={}"
    fileDown = "https://xxgk.eic.sh.cn/xhyf/common/filedown1.do?fileId={}"

    def start_requests(self):
        # 获取企业名称，逐条进行搜索，寻找企业所拥有的项目
        df = pandas.read_excel(self.root_path + 'Enterprises.xlsx', sheet_name="Sheet1", header=0)
        for name in df['name'].values.tolist():
            yield scrapy.FormRequest(url=self.jsxmxxgkInfo_main, formdata={
                "selValue": name,
                "selFieldShowStr": "建设单位",
            }, callback=self.parse_information_disclosure_main, meta={"compName": name})

    def parse_information_disclosure_total(self, response):
        # 获取总页数，处理分页请求
        page_num = response.xpath(r"/html/body/div[@id='wrapper']/div[@class='page']/span[3]/text()").re_first(
            r"共([0-9]*)页")
        page_num = int(page_num)
        for num in range(1, page_num + 1):
            data = {
                "pageSize": str(self.page_size),
                "currentPage": str(num),
                "from": "jsxm",
            }
            yield scrapy.FormRequest(url=self.jsxmxxgkInfo_main, formdata=data,
                                     callback=self.parse_information_disclosure_main, meta=response.meta)

    def parse_information_disclosure_main(self, response):
        # 获取到一页数据表，提交Item
        rows = response.xpath(r"//table[@class='Llist limit']/tr")
        for row in rows:
            url_id = row.xpath(r"@onclick").get()
            if url_id is None:
                continue
            l_row = list()
            cols = row.xpath(r"td/text()")
            for col in cols:
                l_row.append(col.get().strip())
            l_row.append(url_id.split('\'')[1])
            # go to pipline
            item = InformationDisclosureItem()
            item['id'] = int(l_row[0])
            item['project_name'] = l_row[1]
            item['area'] = l_row[2]
            item['current_stage'] = l_row[3]
            item['public_date'] = l_row[4]
            item['project_id'] = l_row[5]
            yield item
            # 请求每一条信息的子页面
            yield scrapy.Request(url=self.subPage.format(l_row[5]), callback=self.parse_subpage,
                                 meta={"project_id": l_row[5], "compName": response.meta["compName"]})

    def parse_subpage(self, response):
        # 处理子页面
        leftBox = response.xpath(r"//div[contains(@class,'leftBox')]")
        rightBox = response.xpath(r"//div[contains(@class,'rightBox')]")
        rightBox.pop(0)  # 去除右边第一个空
        if len(leftBox) == len(rightBox):
            # 左右应当相等，排除意外
            dict_data = dict()
            list_files = list()
            for left in leftBox:
                names = left.xpath(r"div/text()")
                values = left.xpath(r"div/span/text()")
                name_, value_ = list(), list()
                for name in names:
                    name_.append(name.get().strip())
                for value in values:
                    value_.append(value.get().strip())
                left = zip(name_, value_)
                dict_data.update(dict(left))

            for right in rightBox:
                tr = right.xpath(r"div/div/div/table/tr")
                for row in tr:
                    file_name = row.xpath(r"td/text()").get()
                    dict_file = dict()
                    if file_name is not None:
                        file_id = row.xpath(r"td/button/@onclick").get().split('\'')[1]
                        dict_file["file_id"] = file_id
                        dict_file["file_name"] = file_name
                        dict_file["file_url"] = self.fileDown.format(file_id)
                        dict_file["project_id"] = response.meta.get("project_id")
                        dict_file["compName"] = response.meta.get("compName")
                        list_files.append(dict_file)
            # 获取其他关键信息
            dict_data["建设单位"] = response.xpath(
                r'//*[@id="wrapper"]/div[3]/div[3]/div[2]/div[2]/div[2]/div[2]/text()').get()
            dict_data["环评批文文号"] = response.xpath(
                r'//*[@id="wrapper"]/div[3]/div[3]/div[2]/div[2]/div[9]/div[2]/text()').get()
            dict_data["环评项目登记号"] = response.xpath(
                r'//*[@id="wrapper"]/div[3]/div[3]/div[2]/div[2]/div[8]/div[2]/text()').get()

            # 详细表最后一列追加id，方便对应关系
            dict_data["project_id"] = response.meta.get("project_id")

            # 提交项目详情Item
            item = InformationDisclosureDetailItem()
            item["dict_data"] = dict_data
            item["list_files"] = list_files
            print(dict_data, list_files)
            yield item
            # 处理详细页内的文件下载
            file_item = FilesDownloadItem()
            for row in list_files:
                if row.get("file_url") is not None:
                    file_item["file_urls"] = [row["file_url"]]
                    file_item["files"] = [row["file_id"] + "." + row["file_name"].split(".")[-1]]
                    yield file_item
