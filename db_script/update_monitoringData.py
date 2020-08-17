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

from EnvironmentalInformation.common.mssql_tools import DBUtil
from settings import *

schema = "Common"
tab_data = "tab_devicePollutant_data"
tab_ref = "ref_deivceDataMain"
# 加载数据库
db = DBUtil(settings().conf)


def update_monitoringData(row):

    #
    # 数据类型：stJcxm
    pass

