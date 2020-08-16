# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   update_device_baseInfo.py
   Description: 更新设备基础信息表；
                根据设备ID，逐个设备判断更新；
                更新处理设施数据字典表；
   Author:      ljh
   CreateDate:  2020/8/15
-------------------------------------------------
   Change Activity:
                2020/8/15: file created
-------------------------------------------------
    Problem:    deviceType字段没有字典表；
                monitoringType:同一个设备会有多种监测方式;
"""
__author__ = 'ljh'

import uuid

from EnvironmentalInformation.common.mssql_tools import DBUtil
from EnvironmentalInformation.common.update_tools import update_check, get_id_by_insert
from settings import *

schema = "Common"
tablename = "tab_device_baseInfo"
# 加载数据库
db = DBUtil(settings().conf)


def check_monitor():
    # if word == "手工"
    pass


def update_device_baseInfo(row):
    companyId = uuid.uuid5(uuid.NAMESPACE_DNS, row.get('stWryCode'))
    deviceId = uuid.uuid5(companyId, row.get('stSitecode'))
    obj = db.first(schema=schema, table_name=tablename, ft={"deviceId": deviceId})
    deviceType = row["stType"]
    deviceType = get_id_by_insert(db, schema, "dict_deviceType", "deviceTypeName", deviceType, call_key="deviceTypeId")
    # 0：监测计划，1：手工监测值，2：自动监测值，3：环保管家日常巡
    _str = row["stJcff"]
    monitoringType = 0
    if _str.find("计划"):
        monitoringType += 1
    if _str.find("手工"):
        monitoringType += 10
    if _str.find("自动"):
        monitoringType += 100
    if _str.find("管家"):
        monitoringType += 1000
    # 判断uid是否存在
    if obj is None:
        # 创建数据集
        item = {
            "deviceId": deviceId,
            "companyId": companyId,
            "deviceName": row["stSitename"],
            "deviceNo": row["stSitecode"],
            "deviceType": deviceType,  # 关联字典表
            "monitoringType": monitoringType,
        }
        # 新增一条设备数据
        print(item)
        result = db.insert(schema, tablename, item)
        print(result)
        return

    # 判断是否有更新值
    items = dict()
    update_check(obj, "deviceName", row.get("stSitename"), items)
    update_check(obj, "deviceNo", row.get("stSitecode"), items)
    update_check(obj, "deviceType", deviceType, items)
    update_check(obj, "monitoringType", monitoringType, items)
    if len(items) > 0:
        # 写入数据库
        db.update_by_dict(schema, tablename, items, ft={"deviceId": deviceId})


# 动态更新处理设施数据字典表
def update_deviceData_indexes(row):
    # 根据数据名，查询表中是否存在
    name = row.get("stProject")
    obj = db.first(schema, "dict_deviceData_indexes", {"name": name})
    if obj is None:
        uid = uuid.uuid5(uuid.NAMESPACE_DNS, name)
        deviceType = get_id_by_insert(db, schema, "dict_deviceType", "deviceTypeName", row["stType"],
                                      call_key="deviceTypeId")
        item = {
            "ID": uid,
            "name": name,
            "typeId": deviceType,
            "state": 1,
            "unit": row.get("jcxm_dw"),
        }
        db.insert(schema, "dict_deviceData_indexes", [item])
