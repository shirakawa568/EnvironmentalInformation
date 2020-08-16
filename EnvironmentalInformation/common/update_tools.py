# 查字典表，并动态插入新数据，返回ID
def get_id_by_insert(db, schema, tablename, key, value, call_key):
    obj = db.first(schema, tablename, ft={key: value})
    if obj is None:
        db.insert(schema, tablename, {key: value})
    obj = db.first(schema, tablename, ft={key: value})
    return eval(f"obj.{call_key}")


# 查字典表，返回ID
def get_id_by_dict(db, schema, tablename, key, value, call_key):
    """
    获取ID，通过查询字典表，未查询到，则返回空
    :param db: 数据库对象
    :param schema: 数据库架构
    :param tablename: 表名
    :param key: 查询的键
    :param value: 查询的值
    :param call_key: 返回的键
    :return:
    """
    obj = db.first(schema, tablename, ft={key: value})
    return "" if obj is None else eval(f"obj.{call_key}")


#
def update_check(obj, key, col, items: dict):
    if eval(f"obj.{key}") != col:
        items.update({key: col})


# 字符转换
def if_(key):
    if key == "是":
        key = 1
    elif key == "否":
        key = 0
    else:
        key = 0
    return key

