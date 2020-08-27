# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   test
   Description:
   Author:      shira
   CreateDate:  2020/8/26
-------------------------------------------------
   Change Activity:
                2020/8/26:
-------------------------------------------------
"""
__author__ = 'shira'

import datetime
import json

import requests
url = "https://xxgk.eic.sh.cn/jsp/view/waterAuto/list.do"
# url = "https://xxgk.eic.sh.cn/jsp/view/wasteAuto/list.do"
wryCode = "607335456001"

now_time = datetime.datetime.now()
month = now_time.month
startDate = now_time + datetime.timedelta(days=-1)
endDate = now_time

text = requests.post(url=url, data={"order": "asc",
                                   "pscode": wryCode,
                                   "wryCode": wryCode,
                                   "pageSize": "10",
                                   "currentPage": "1",
                                   "beginDate": startDate.strftime('%Y-%m-%d %H:%M:%S'),
                                   "endDate": endDate.strftime('%Y-%m-%d %H:%M:%S'),
                                   }).text
rows = json.loads(text).get("rows")
count = len(rows)
for item in rows:
    print(item)
