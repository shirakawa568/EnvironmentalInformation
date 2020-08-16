# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   update_tab_company_baseInfo.py
   Description: 更新企业基础信息表；
                在爬虫进行过程中进行数据更新；
   Author:      ljh
   CreateDate:  2020/8/15
-------------------------------------------------
   Change Activity:
                2020/8/15: file created
                2020/08/16: 脚本完成
-------------------------------------------------
"""
__author__ = "ljh"

import datetime
import uuid

from EnvironmentalInformation.common.mssql_tools import DBUtil
from EnvironmentalInformation.common.update_tools import *
from settings import *

schema = "Common"
tablename = "tab_company_baseInfo"
# 加载数据库
db = DBUtil(settings().conf)


def update_company_baseInfo(row):
    uid = uuid.uuid5(uuid.NAMESPACE_DNS, row.get('污染源编码', ''))
    obj = db.first(schema=schema, table_name=tablename, ft={"companyId": uid})
    # 判断uid是否存在
    if obj is None:
        industryType1 = get_id_by_insert(db, "Common", "dict_industry", "industryTypeName", row.get('行业大类', ),
                                         "industryType")
        industryType2 = get_id_by_insert(db, "Common", "dict_industry", "industryTypeName", row.get('所属小类', ),
                                         "industryType")
        # 数据集插入数据
        item = {
            "companyId": uid,  # 企业id（主键）
            "companyName": row.get('单位名称', ''),  # 单位名称
            "zch": row.get('注册号/统一社会信用代码', ''),  # 注册号/统一社会信用代码
            "leader": row.get('法定代表人', ''),  # 法定代表人
            "ifSs": if_(row.get('是否属上市公司', '')),  # 是否属上市公司
            "establishDate": row.get('成立日期', '0'),  # 成立日期
            "businessScope": row.get('经营范围', ''),  # 经营范围
            "pollutionName": row.get('污染源名称', ''),  # 污染源名称
            "pollutionCode": row.get('污染源编码', ''),  # 污染源编码
            "pollutionType": row.get('污染源类别', ''),  # 污染源类别
            "industryType1": industryType1,  # 行业大类（字典表）
            "industryType2": industryType2,  # 所属小类（字典表）
            "address": row.get('生产地址', ''),  # 生产地址
            "districtCode": get_id_by_dict(db, 'Common', 'dict_district', 'districtName', '行政区划', "districtCode"),
            # 行政区划
            "department": row.get('主管部门', ''),  # 主管部门
            "contactPerson": row.get('联系人', ''),  # 联系人
            "contactTel": row.get('联系电话', ''),  # 联系电话
            "email": row.get('电子邮箱', ''),  # 电子邮箱
            "ifHfqgxkz": if_(row.get('是否已核发全国许可证', '')),  # 是否已核发全国许可证
            "longitude": row.get('地理坐标(经度)', ''),  # 地理坐标(经度)
            "latitude": row.get('地理坐标(纬度)', ''),  # 地理坐标(纬度)
            "cycleTime": "".join(row.get('生产周期', '').split()),  # 生产周期
            "ifTc": if_(row.get('是否停产', '')),  # 是否停产
            "ifDayPull": if_(row.get('是否日排放量100立方米以上的直排海污染源', '')),  # 是否日排放量100立方米以上的直排海污染源
            "productIntroduction": row.get('生产情况简介', ''),  # 生产情况简介
            # "scale": row.get('',''),  # 企业规模

            # 建设项目中后期信息公开
            # "approvalNoOfEIAReport": row.get('',''),  # 环境影响评价报告批复批文号
            # "ifEnvironApprove": row.get('',''),  # 是否取得环境影响评价报告批复
            # "ifRegisterPutFile": row.get('',''),  # 是否登记表备案（留空）
            # "registerPutFileNo": row.get('',''),  # 登记表备案号（留空）
            # "environCompletionAcceptance": row.get('',''),  # 环保自主竣工验收情况（1-已验收/2-未验收）
            # "environCompletionPublicity": row.get('',''),  # 环保自主竣工验收平台公示情况（1已公示/2未公示）

            # 许可信息公开
            # "pwCardApplication": row.get('',''),  # 国家排污许可证申领情况（1已申领/2未申领）
            # "pwCardNumber": row.get('',''),  # 国家排污许可证编号
            # "pwCardTerm": row.get('',''),  # 国家排污许可证有效期

            # 应急预案
            # "environEmergencyPlan": row.get('',''),  # 环境应急预案编制情况（1已编制/2未编制）
            # "environEmergencyPlanNumber": row.get('',''),  # 环境应急预案备案号

            # "totalValue": row.get('',''),  # 工业总产值（当年价格）
            "ifImportentPwCompany": 1,  # 是否为重点排污单位
            # "ifBigRisk": row.get('',''),  # 是否存在重大风险源
            # "ifOnePollutionDischarge": row.get('',''),  # 是否涉及一类污染物排放
            # "employeesNumber": row.get('',''),  # 企业员工数
            "updateTime": datetime.datetime.now(),  # 更新时间
            "updateUserId": 'E4CE162A-8D0B-451F-B7B9-0D99CA4F5BB8',  # 更新人
        }
        # 写入数据库
        result = db.insert("Common", "tab_company_baseInfo", [item])
        return

    # 判断是否有更新值
    items = dict()
    update_check(obj, "companyName", row.get("单位名称"), items)
    update_check(obj, "zch", row.get("注册号/统一社会信用代码"), items)
    update_check(obj, "leader", row.get("法定代表人"), items)
    update_check(obj, "ifSs", if_(row.get("是否属上市公司")), items)
    try:
        establishDate = datetime.datetime.strptime(row.get("成立日期"), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        establishDate = datetime.datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    update_check(obj, "establishDate", establishDate, items)
    update_check(obj, "businessScope", row.get("经营范围"), items)
    update_check(obj, "pollutionName", row.get("污染源名称"), items)
    update_check(obj, "pollutionCode", row.get("污染源编码"), items)
    update_check(obj, "pollutionType", row.get("污染源类别"), items)
    update_check(obj, "industryType1", get_id_by_insert(db, "Common", "dict_industry", "industryTypeName",
                                                        row.get('行业大类', ), "industryType"), items)
    update_check(obj, "industryType2", get_id_by_insert(db, "Common", "dict_industry", "industryTypeName",
                                                        row.get('所属小类', ), "industryType"), items)
    update_check(obj, "address", row.get("生产地址"), items)
    update_check(obj, "districtCode", get_id_by_dict(db, 'Common', 'dict_district', 'districtName',
                                                     row.get("行政区划"), "districtCode"), items)
    update_check(obj, "department", row.get("主管部门"), items)
    update_check(obj, "contactPerson", row.get("联系人"), items)
    update_check(obj, "contactTel", row.get("联系电话"), items)
    update_check(obj, "email", row.get("电子邮箱"), items)
    update_check(obj, "ifHfqgxkz", if_(row.get("是否已核发全国许可证")), items)
    update_check(obj, "longitude", float(row.get("地理坐标(经度)")), items)
    update_check(obj, "latitude", float(row.get("地理坐标(纬度)")), items)
    update_check(obj, "cycleTime", "".join(row.get('生产周期', '').split()), items)
    update_check(obj, "ifTc", if_(row.get("是否停产")), items)
    update_check(obj, "ifDayPull", if_(row.get("是否日排放量100立方米以上的直排海污染源")), items)
    update_check(obj, "productIntroduction", row.get("生产情况简介"), items)
    # 有更新的数据，则修改更新时间与用户ID
    if len(items) > 0:
        items.update({"updateTime": datetime.datetime.now(),  # 更新时间
                      "updateUserId": settings().userId})  # 更新人
        # 写入数据库
        db.update_by_dict("Common", "tab_company_baseInfo", items, ft={"companyId": uid})
