# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   update_informationDisclosure
   Description: 事中事后信息更新入库
   Author:      ljh
   CreateDate:  2020/8/28
-------------------------------------------------
   Change Activity:
                2020/8/28:
-------------------------------------------------
"""
__author__ = 'ljh'

import datetime
import uuid

import pandas

from EnvironmentalInformation.common.mssql_tools import DBUtil
from EnvironmentalInformation.common.tools import get_root_path
from settings import *

# 加载数据库
db = DBUtil(settings().conf)

schema = "Attachment"
tab_files = "tab_companyFile"

df = pandas.read_excel(get_root_path("EnvironmentalInformation") + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)


def update_information_disclosure(row):
    """
    更新企业项目事中事后附件文件
    :param row:
    :return:
    """
    wry_code = df.loc[df['单位名称'] == row.get('compName'), "污染源编码"].iloc[0]
    if len(wry_code) == 0:
        print(row.get('建设单位'), "未找到")
        return
    companyId = uuid.uuid5(uuid.NAMESPACE_DNS, wry_code)

    # 更新附件表
    fileId = row.get('file_id', None)
    fileName = row.get('file_name')
    filePath = get_root_path("EnvironmentalInformation") + "拟报批的环境影响报告表\\" + fileName
    obj = db.first(schema=schema, table_name=tab_files, ft={"fileId": fileId})
    if obj is None:
        item = {
            "fileId": fileId,
            "companyId": companyId,
            "fileName": fileName,
            "filePath": filePath,
            "fileType": 2,  # 1.应急预案;2.拟报批的环境影响报告表
            "updateTime": datetime.datetime.now(),  # 更新时间
            "updateUserId": settings().userId,  # 更新人
        }
        result = db.insert("Attachment", tab_files, [item])
        print(result)
