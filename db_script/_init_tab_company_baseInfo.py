# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   _init_tab_company_baseInfo.py
   Description: 初始化企业详细信息表
   Author:      ljh
   CreateDate:  2020/8/12
-------------------------------------------------
   Change Activity:
                2020/8/12: file created
-------------------------------------------------
"""
__author__ = 'ljh'

import uuid

import pandas

from EnvironmentalInformation.common.mssql_tools import DBUtil

# 加载数据库
from EnvironmentalInformation.common.tools import get_root_path

conf = "mssql+pymssql://sa:Imanity.568312@127.0.0.1/Environment"
db = DBUtil(conf)

"""
企业基础信息表中需要写入的信息：
"""
# 读取文件
df = pandas.read_excel(get_root_path("EnvironmentalInformation") + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
list_data = list()
for index, row in df.iterrows():
    companyName = row['单位名称']
    uid = uuid.uuid5(uuid.NAMESPACE_DNS, companyName)

    if index > 0:
        if companyName == list_data[-1].get("companyName"):
            print(companyName)
            continue
    list_data.append({"companyId": uid,
                      "companyName": companyName,
                      })

print(list_data)
result = db.insert("Common", "tab_company_baseInfo", list_data)
print(result)


# 动态扩增字典表数据
def add_dict_table(db, schema, table_name, data):
    """
    向字典表新增数据，过滤已存在的数据
    :param db: 数据库对象
    :param schema: 字符串
    :param table_name: 字符串
    :param data: 字典数据（一条）
    :return:
    """
    result = db.insert(schema, table_name, data)
    logger
