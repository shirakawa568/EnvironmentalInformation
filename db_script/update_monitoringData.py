# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   update_monitoringData.py
   Description: 更新处理设施监测数据，每日更新近24小时，不一定每天有数据更新;
                同时更新关联表；
   Author:      ljh
   CreateDate:  2020/8/17
-------------------------------------------------
   Change Activity:
                2020/8/17: file created
-------------------------------------------------
    Problem:    None
"""
__author__ = 'ljh'

import datetime
import uuid

from EnvironmentalInformation.common.mssql_tools import DBUtil
from settings import *

schema = "Data"
tab_data = "tab_devicePollutant_data"
tab_ref = "ref_deivceDataMain"
# 加载数据库
db = DBUtil(settings().conf)


def update_monitoringData(row, title, wryCode):
    # 数据的唯一标识
    dataId = uuid.uuid4()

    # 计算deviceId
    companyId = uuid.uuid5(uuid.NAMESPACE_DNS, wryCode)
    # 根据处理设施名称生成deviceId
    d_id = row.get('stJcdCode', None)
    if d_id == None:  # 如果没有获取到，则换一种列表明再获取
        d_id = row.get('FACTORNAME', None)
    deviceId = uuid.uuid5(companyId, d_id)
    # 计算污染源类型与处理设施类型关联表ID
    name = row.get("stJcxm", None)
    if name == None:
        name = row.get("stJcxm", None)
    pollutantId = uuid.uuid5(deviceId, name)
    # 创建数据集
    item = {
        "dataId": dataId,
        "pollutantId": pollutantId,
        "val": row["stJcnd"],
        "updateTime": datetime.datetime.now(),
        "updateUserId": settings().userId,  # 关联字典表
    }
    # 新增一条设备数据
    print(item)
    result = db.insert(schema, tab_data, [item])
    print(result)
