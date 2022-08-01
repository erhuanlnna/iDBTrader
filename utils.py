# 此文件用于用户记录
from dataset import select

# 通过用户名，获取用户记录，如果不存在，则返回None
def query_user(username):
    # 先获取数据库用户信息
    users = select('select * from User where Name = \'%s\'' %username,'transaction')
    for user in users:
        if user['Name'] == username:
            return user

def search_all_saledataset():
    #查询所有在售的数据
    all_data = select('select * from Dataset where State = 1', 'transaction')
    #返回一个列表， 列表元素为字典
    return all_data;

def search_seller_dataset(Name):
    #查询某个seller的的数据
    all_data = select('select * from Dataset where Owner = \'%s\''%Name, 'transaction')
    #返回一个列表， 列表元素为字典
    return all_data;

def search_dataset(keyword, did, seller_name, begin_date, end_date):
    sql="SELECT * FROM Dataset WHERE Name like "+ "\'"+"%" + "%s"%keyword + "%"+ "\'"
    if did != "":
        sql += " and DID = %s"%did
    if seller_name != "":
        sql += " and Owner like "+ "\'"+"%" + "%s"%seller_name + "%"+ "\'"
    if begin_date != "":
        sql += " and CreateDate >= " + "\'%s\'"%begin_date
    if end_date != "":
        sql += " and CreateDate <= " + "\'%s\'"%end_date
    # print(sql)
    return select(sql, "transaction")

# 判断sql语句的正确性
# def isLegal(sql):
#     return True

# 定价函数
def getSQLPrice():
    return 1

def searchOrderOfBuyer(Buyer):
    all_data = select('select * from order_table where Buyer = \'%s\''%Buyer, 'transaction')
    #返回一个列表， 列表元素为字典
    return all_data;
def searchOrderOfSeller(Seller):
    all_data = select('select * from order_table where Seller = \'%s\''%Seller, 'transaction')
    #返回一个列表， 列表元素为字典
    return all_data;
def searchALLOrder():
    all_data = select('select * from order_table', 'transaction')
    #返回一个列表， 列表元素为字典
    return all_data;


def search_order(Datasetname, oid, buyer_name,seller_name, begin_date, end_date):
    sql="SELECT * FROM order_table WHERE DName like "+ "\'"+"%" + "%s"%Datasetname + "%"+ "\'"
    if oid != "":
        sql += " and OID = %s"%oid
    if seller_name != "":
        sql += " and Seller like "+ "\'"+"%" + "%s"%seller_name + "%"+ "\'"
    if buyer_name != "":
        sql += " and Buyer like "+ "\'"+"%" + "%s"%buyer_name + "%"+ "\'"
    if begin_date != "":
        sql += " and CreateDate >= " + "\'%s\'"%begin_date
    if end_date != "":
        sql += " and CreateDate <= " + "\'%s\'"%end_date
    # print(sql)
    return select(sql, "transaction")

