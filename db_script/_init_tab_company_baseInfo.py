# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:   _init_tab_company_baseInfo.py
   Description: 初始化企业详细信息表；
                动态的向行业类别表插入了初始数据；
   Author:      ljh
   CreateDate:  2020/8/12
-------------------------------------------------
   Change Activity:
                2020/8/12: file created
                2020/8/15: update: 更新了“环评事中事后信息公开”中的数据插入
                2020/8/16: delete: 请使用update_tab_company_baseInfo.py脚本动态更新
-------------------------------------------------
"""
__author__ = 'ljh'

import datetime
import uuid

import pandas

from EnvironmentalInformation.common.mssql_tools import DBUtil
from EnvironmentalInformation.common.tools import get_root_path

# 加载数据库
conf = "mssql+pymssql://sa:Imanity.568312@192.168.31.11/Environment"
db = DBUtil(conf)


def if_(key):
    if key == "是":
        key = 1
    elif key == "否":
        key = 0
    else:
        key = ''
    return key


def get_id_by_dict(schema, tablename, key, keyname, call_key):
    """获取ID，通过查询字典表，未查询到，则返回空"""
    obj = db.first(schema, tablename, ft={key: row.get(keyname)})
    return "" if obj is None else eval(f"obj.{call_key}")


"""
企业基础信息表中需要写入的信息：
"""
# 读取文件
df = pandas.read_excel(get_root_path("EnvironmentalInformation") + 'Enterprises.xlsx', sheet_name="企业详细信息", header=0)
df = df.fillna(value='')

# 创建数据集
list_data = list()
for index, row in df.iterrows():
    uid = uuid.uuid5(uuid.NAMESPACE_DNS, row.get('污染源编码', ''))  # 使用污染源编码生成GUID
    # 获取行业类别
    industryType1 = db.first('Common', 'dict_industry', ft={"industryTypeName": row.get('行业大类')})
    industryType2 = db.first('Common', 'dict_industry', ft={"industryTypeName": row.get('所属小类')})
    if industryType1 is None:
        db.insert('Common', 'dict_industry',
                  {"industryTypeName": row.get('行业大类'),
                   "state": 1})
        industryType1 = db.first('Common', 'dict_industry', ft={"industryTypeName": row.get('行业大类')}).industryType
    else:
        industryType1 = industryType1.industryType
    if industryType2 is None:
        db.insert('Common', 'dict_industry',
                  {"parentType": industryType1,
                   "industryTypeName": row.get('所属小类'),
                   "state": 1})
        industryType2 = db.first('Common', 'dict_industry', ft={"industryTypeName": row.get('所属小类')}).industryType
    else:
        industryType2 = industryType2.industryType
    # 获取环评事中事后信息公开相关信息
    df_project = pandas.read_excel(get_root_path("EnvironmentalInformation") + 'InformationDisclosure.xlsx',
                                   sheet_name="项目详情", header=0)
    data = df.loc[df['单位名称'] == "3M中国有限公司", "行业大类"]
    print(type(data))

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
        "districtCode": get_id_by_dict('Common', 'dict_district', 'districtName', '行政区划', "districtCode"),  # 行政区划
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

        # 应急预案（已在应急预案的pipeline中更新入库）
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
    list_data.append(item)
print(list_data)

# 写入数据库
result = db.insert("Common", "tab_company_baseInfo", list_data)
print(result)
