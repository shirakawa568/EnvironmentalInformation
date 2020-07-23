import scrapy


class EnterprisesInfoSpider(scrapy.Spider):
    name = 'enterprises_info'
    # allowed_domains = ['xxgk.eic.sh.cn']
    start_urls = ['https://xxgk.eic.sh.cn/jsp/view/list.jsp']

    def parse(self, response):
        page_num = response.xpath(r"/html/body/div[@id='wrapper']/div[@class='page']/span[3]/text()").re_first(
            r"共([0-9]*)页")
        page_num = int(page_num)
        for num in range(1, page_num + 1):
            data = {
                "currentPage": str(num)
            }
            url = self.start_urls[0]
            print(url, data)
            yield scrapy.FormRequest(url=url, formdata=data, callback=self.parse_index_page)

    def parse_index_page(self, response):
        rows = response.xpath(r"//table[@class='Llist limit']/tr")
        for row in rows:
            cols = row.xpath(r"/tr/td/text()")

