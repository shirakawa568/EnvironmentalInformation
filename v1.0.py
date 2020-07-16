"""
EnvironmentalInformation__v1.0 by ruino
CREATE:20-7-15 

"""
import json
import random
import re
import sys
import time

import requests
from fake_useragent import UserAgent
from lxml import etree


class AnimateSpider:
    def __init__(self):
        self.host = 'https://xxgk.eic.sh.cn/tbBase/proList.do?selField=ST_EXT1&selValue=&district=&nd=2020'
        self.host2 = 'http://permit.mee.gov.cn/permitExt/syssb/xkgg/xkgg!licenseInformation.action'

    @staticmethod
    def get_html(url) -> str:
        """
        获取响应内容
        :return: 返回字符串
        """
        headers = {'User-Agent': UserAgent().random}
        res = requests.get(url=url, headers=headers, timeout=10)
        html = res.text
        return html

    @staticmethod
    def post_html(url, data):
        headers = {'User-Agent': UserAgent().random}
        res = requests.post(url=url, headers=headers, data=data, timeout=10)
        html = res.text
        return html

    @staticmethod
    def get_json(url) -> str:
        headers = {'User-Agent': UserAgent().random}
        res = requests.get(url=url, headers=headers, timeout=10)
        s_json = res.text
        d_json = json.loads(s_json)
        return d_json

    @staticmethod
    def parse_html_by_reg(html, r_str) -> list:
        """
        解析提取html中该数据，
        :param r_str: str   正则表达式
        :param html: str    响应内容
        :return: list   返回解析数据列表
        """
        pattern = re.compile(r_str, re.S)
        r_list = pattern.findall(html)
        return r_list

    @staticmethod
    def parse_html_by_xpath(html, xpath) -> list:
        """
        解析响应内容的数据
        :param html: str    响应内容
        :param xpath: str   xpath表达式
        :return: 返回解析的数据列表
        """
        try:
            parse_html = etree.HTML(html)
            res = parse_html.xpath(xpath)
        except Exception as e:
            print("error:", e)
        else:
            return res

    def save_data(self, name, data):
        """
        保存数据
        :return:
        """
        pass

    def run(self) -> int:
        """
        入口函数,整体逻辑控制
        :return:
        """
        list_ = list()
        html = self.post_html(self.host2, {'page.pageNo': 9})
        totle = self.parse_html_by_reg(html, r'共[0-9]*页')
        rows = self.parse_html_by_xpath(html, '//table/tr')
        for row in rows:
            list_.append(row.xpath('td/text()'))
        print(list_)


if __name__ == '__main__':
    ms = AnimateSpider()
    totel_count = ms.run()
    # print("总计：{}条".format(totel_count))
