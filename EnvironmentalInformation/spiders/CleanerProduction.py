import datetime

import scrapy

from EnvironmentalInformation.common.tools import get_root_path
from EnvironmentalInformation.items import CleanerProductionItem, FilesDownloadItem


class CleanerProductionSpider(scrapy.Spider):
    name = 'CleanerProduction'

    root_path = get_root_path('EnvironmentalInformation')
    today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    year = datetime.datetime.now().year
    lastYear = year - 1

    # 自定义配置
    custom_settings = {
        'ITEM_PIPELINES': {
            'EnvironmentalInformation.pipelines.EmergencyPlanFilePipeline': 100,
            'EnvironmentalInformation.pipelines.CleanerProductionPipeline': 200,
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'{root_path}log\\CleanerProduction-{today}.log',
        'FILES_STORE': f'{root_path}清洁生产'
    }

    # 分页参数
    page_size = 20

    # 环评事中事后信息公开 列表
    qjsc = "https://xxgk.eic.sh.cn/jsp/view/qjsc/qjsc.jsp"
    qjscDetail = "https://xxgk.eic.sh.cn/jsp/view/qjsc/qjscDetail.jsp?year={}&stId={}"
    qjscFile = "https://xxgk.eic.sh.cn/qjsc/cmpy/syDownload.do?stSyId={}"

    def start_requests(self):
        data = {
            "year": str(self.lastYear),
            "maxYear": str(self.year),
            "pageSize": str(self.page_size),
        }
        yield scrapy.FormRequest(url=self.qjsc, formdata=data, callback=self.parse_cleaner_production_total)

    def parse_cleaner_production_total(self, response):
        page_num = int(response.xpath(r'//*[@id="form1"]/div/div[2]/div[1]/span[1]/text()').get())
        for num in range(1, page_num + 1):
            data = {
                "year": str(self.lastYear),
                "maxYear": str(self.year),
                "pageSize": str(self.page_size),
                "currentPage": str(num),
            }
            yield scrapy.FormRequest(url=self.qjsc, formdata=data,
                                     callback=self.parse_cleaner_production_main)

    def parse_cleaner_production_main(self, response):
        # 获取子页面地址
        rows = response.xpath(r"//div[@class='qj-txt']/a/@href")
        for row in rows:
            url_id = row.get().split('\'')[1]
            # 请求每一条信息的子页面
            yield scrapy.Request(url=self.qjscDetail.format(self.lastYear, url_id), callback=self.parse_subpage)

    def parse_subpage(self, response):

        if response.status == 200 and response.text != '':
            crop_name = response.xpath(r"/html/body/div[2]/div[2]/div[1]/div[1]/div/text()").get().strip()
            crop_id = response.xpath(r"/html/body/div[2]/div[2]/div[1]/div[5]/div/text()").get().strip()
            double = response.xpath(r"/html/body/div[2]/div[1]").get()
            double_super = double_have = ""
            # 具有双超信息
            double_super_data = list()
            if "双超信息" in double:
                double_super = "双超"
                # 获取双超数据
                data = response.xpath(r"/html/body/div[2]/div[2]/div[2]").get()
                if "企业未公示" in data:
                    double_super_data = [{"排放污染物名称": "企业未公示", "crop_id": crop_id}]
                else:
                    th = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[2]/table/tr/th/text()")
                    td = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[2]/table/tr/td/text()")
                    for i in range(len(td) // len(th)):
                        dict_data = dict()
                        for j in range(len(th)):
                            dict_data[th[j].get().strip()] = td[i * 3 + j].get()
                        dict_data["crop_id"] = crop_id
                        double_super_data.append(dict_data)

            # 具有双有信息
            use_materials_data, discharge_materials_data, generation_disposal_data = list(), list(), list()
            filename = ""
            if "双有信息" in double:
                double_have = "双有"
                # 获取 使用有毒有害原料
                data = response.xpath(r"/html/body/div[2]/div[2]/div[2]").get()
                if "企业未公示" in data:
                    use_materials_data = [{"名称": "企业未公示", "crop_id": crop_id}]
                    discharge_materials_data = [{"名称": "企业未公示", "crop_id": crop_id}]
                    double_super_data = [{"危险废物名称": "企业未公示", "crop_id": crop_id}]
                else:
                    th = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[2]/table/tr/th/text()")
                    td = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[2]/table/tr/td/text()")
                    for i in range(len(td) // len(th)):
                        dict_data = dict()
                        for j in range(len(th)):
                            dict_data[th[j].get().strip()] = td[i * 3 + j].get()
                        dict_data["crop_id"] = crop_id
                        use_materials_data.append(dict_data)
                    # 排放有毒有害物质
                    th = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[3]/table/tr/th/text()")
                    td = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[3]/table/tr/td/text()")
                    for i in range(len(td) // len(th)):
                        dict_data = dict()
                        for j in range(len(th)):
                            dict_data[th[j].get().strip()] = td[i * 3 + j].get()
                        dict_data["crop_id"] = crop_id
                        discharge_materials_data.append(dict_data)

                    # 危险废物的产生和处置
                    th = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[4]/table/tr/th/text()")
                    td = response.xpath(r"/html/body/div[2]/div[2]/div[2]/div[4]/table/tr/td/text()")
                    for i in range(len(td) // len(th)):
                        dict_data = dict()
                        for j in range(len(th)):
                            dict_data[th[j].get().strip()] = td[i * 3 + j].get()
                        dict_data["crop_id"] = crop_id
                        generation_disposal_data.append(dict_data)

                    # 环境风险防控措施 文件下载
                    file_url = response.xpath(r"/html/body/div[2]/div[2]/div/div[5]/ul/li/input/@onclick").get().split(
                            '\'')[1]
                    filename = file_url.split("=")[1]
                    fileItem = FilesDownloadItem()
                    fileItem["file_urls"] = [file_url]
                    fileItem["files"] = [filename + ".pdf"]
                    yield fileItem

            item = CleanerProductionItem()
            item["crop_info"] = {"crop_name": crop_name, "crop_id": crop_id, "double_super": double_super,
                                 "double_have": double_have, "file": filename}  # 公司信息:名称、ID、双超、双有
            item["double_super"] = double_super_data
            item["use_materials"] = use_materials_data
            item["discharge_materials"] = discharge_materials_data
            item["generation_disposal"] = generation_disposal_data

            yield item
