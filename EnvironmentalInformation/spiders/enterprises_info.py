import scrapy

from EnvironmentalInformation.items import EnterprisesItem


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

    @staticmethod
    def parse_index_page(response):
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
            item = EnterprisesItem()
            item['id'] = int(l_row[0])
            item['name'] = l_row[1]
            item['area'] = l_row[2]
            item['type'] = l_row[3]
            item['url_id'] = l_row[4]
            yield item

