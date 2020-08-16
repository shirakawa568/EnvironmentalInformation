# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   update_emergency_plan.py
   Description: 保存应急预案相关信息与附件
                涉及企业基础信息表、附件类型字典表、附件表三张表
   Author:      ljh
   CreateDate:  2020/8/15
-------------------------------------------------
   Change Activity:
                2020/8/15: file created and complete
-------------------------------------------------
    Problem:    应急预案备案号没有获取来源，仅通过是否有备案文件判断已编制
"""
__author__ = 'ljh'

import datetime
import uuid

from EnvironmentalInformation.common.mssql_tools import DBUtil
from EnvironmentalInformation.common.tools import get_root_path
from settings import *

schema = "Common"
tablename = "tab_company_baseInfo"
dict_file_type = "dict_fileType"
tab_files = "tab_companyFile"
db = DBUtil(settings().conf)


def update_emergency_plan(row):
    # "environEmergencyPlan": row.get('',''),  # 环境应急预案编制情况（1已编制/2未编制）
    # "environEmergencyPlanNumber": row.get('',''),  # 环境应急预案备案号
    # fileId
    # companyId
    # fileName
    # filePath
    # fileType
    # updateTime
    # updateUserId

    # 更新企业基础信息表
    companyId = uuid.uuid5(uuid.NAMESPACE_DNS, row.get('stWrybh', ''))
    obj = db.first(schema=schema, table_name=tablename, ft={"companyId": companyId})
    if obj:
        if obj.ifRegisterPutFile != 1:
            item = {
                "ifRegisterPutFile": 1,
                "updateTime": datetime.datetime.now(),  # 更新时间
                "updateUserId": settings().userId,  # 更新人
            }
            print(item)
            # 写入数据库
            result = db.update_by_dict(schema, tablename, item, ft={"companyId": companyId})
            print(result)

    # 更新附件表
    fileId = row.get('fileId')
    filePath = get_root_path("EnvironmentalInformation") + "应急预案\\" + row.get('filename')
    obj = db.first(schema="Attachment", table_name=tab_files, ft={"fileId": fileId})
    if obj is None:
        item = {
            "fileId": fileId,
            "companyId": companyId,
            "fileName": row.get('filename'),
            "filePath": filePath,
            "fileType": 1,  # 1.应急预案
            "updateTime": datetime.datetime.now(),  # 更新时间
            "updateUserId": settings().userId,  # 更新人
        }
        result = db.insert("Attachment", tab_files, [item])
        print(result)
