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
