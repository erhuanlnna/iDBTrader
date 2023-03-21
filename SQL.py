import re

import pickle
import datetime
import math
import os
import numpy as np
import pandas as pd
from functools import reduce
import dataset as da
from _ast import If

#########  Interface ############
# Description: 分割sql语句
# Input：sql_statement: 想要分割的sql语句
# Return:   table_list: 查询的表
#           split_point: 用来区分 attribute 和id0的index的值
#           projection_flag: 0表示不是projection查询， 1表示是projection查询
#           new_sql_statement: 更改后的sql语句
#           projections_attributes: attributes的list
#################################
def parse_sql_statements(sql_statement : str):
    # remove the ';'
    sql_statement = sql_statement.replace(';', '')
    
    projection_flag = 0
    # check whether there is DISTINCT condition
    if 'distinct' in sql_statement:
        projection_flag = 1

    if 'where' in sql_statement:
        rule_projections = r'from(.*?)where'
    else:
        rule_projections = r'(?<=from).*$'
    table_list = re.findall(rule_projections, sql_statement)
    table_list = table_list[0].replace(' ','').split(',')
    table_id = [str(i) + '.id0 as id_0, ' + str(i) +'.empty_num as emptynum_0'  for i in table_list ]

    new_sql_statement = sql_statement.replace('distinct ', '')
    location = new_sql_statement.index("from") - 1
    str_list = list(new_sql_statement)
    str_list.insert(location, ', '+', '.join(table_id) )
    new_sql_statement = ''.join(str_list)

    # obtain the attributes in where clause
    where_attributes = []
    
    if 'where' in sql_statement:
        search_attributes = re.findall(r'(?<=where).*$', sql_statement) #取where后面的字符串
        search_attributes = search_attributes[0].replace(' ','').split('and')   #以and分割
        for each_equ in search_attributes:
            equ_list = []
            if '>=' in each_equ:
                equ_list = each_equ.split('>=')
            elif '<=' in each_equ:
                equ_list = each_equ.split('<=')
            elif '<>' in each_equ:
                equ_list = each_equ.split('<>')
            elif '>' in each_equ:
                equ_list = each_equ.split('>')
            elif '<' in each_equ:
                equ_list = each_equ.split('<')
            elif '=' in each_equ:
                equ_list = each_equ.split('=')
            for attr in equ_list:
                where_attributes.append(attr) #不是attribute的会在get_attribute_list这个函数里被剔除，所以在这边可以全部append进去
                
    # print(new_sql_statement)
    # print(where_attributes)
    return table_list,projection_flag, new_sql_statement, where_attributes




def cal_uca_price(no_duplicate_lineage_set : list, table_list:list, db):

    price = 0
    certain_lineage_num = 0
    total_completeness = 0
    base_price_list = []
    
    for i in range(len(table_list)):
        empty_sum = 0
        no_duplicate_lineage_set_this_table = set()
        complteness_this_table = 0
        base_price = da.select("select BasePrice from Dataset where Name = '%s'"%table_list[i], 'transaction')
        base_price = float(base_price[0]["BasePrice"])
        # print(base_price)
        base_price_list.append(base_price)
        # -2是因为减去id0和empty_num这两个原来不存在在数据里的值
        attribute_num = da.select("SELECT COUNT(*) FROM information_schema. COLUMNS WHERE table_schema = '%s' AND table_name = '%s'"%(db,table_list[i]),db)
        attribute_num = attribute_num[0]['COUNT(*)'] - 2
        lineage_num = len(no_duplicate_lineage_set[i])
        # print(np.array(no_duplicate_lineage_set[i]))
        # print(np.sum(np.array(no_duplicate_lineage_set[i]), axis = 0))
        empty_sum = np.sum(np.array(no_duplicate_lineage_set[i]), axis = 0)[1]
        complteness_this_table = 1 - empty_sum /(attribute_num*lineage_num)
        total_completeness += complteness_this_table
        price += base_price * complteness_this_table * lineage_num # 使用不重复的lineage个数
        certain_lineage_num += lineage_num # 使用不重复的lineage个数

    total_completeness = total_completeness/len(table_list)
    return price, certain_lineage_num, total_completeness, base_price_list

def cal_quca_price(no_duplicate_lineage_set : list, table_list:list, attributes_list, sensitivity: float, price_coefficient: float, result_num: int, db): 
    
    uca_price,certain_lineage_num,total_completeness,base_price_list = cal_uca_price(no_duplicate_lineage_set, table_list, db)
    print('computing UCA price', uca_price)
    # 计算查询质量，需要计算在attribute_list上为空的元素
    uncertain_tuple_num = 0
    uncertain_quality = 0
    # print('------------')
    # print(attributes_list)
    for i in range(len(attributes_list)):
        table_name = table_list[i]
        is_null_sql = f''
        for j in range(len((attributes_list[i]))):
            is_null_sql += f'isnull({attributes_list[i][j]})+'            
        is_null_sql=is_null_sql.strip('+') #去掉最后一个加号
        search_sql = f"Select {is_null_sql} from {table_name} where {is_null_sql}<>0"
        # print(search_sql)
        results  = da.select(search_sql, db)
        null_list = []
        for each_null in results:
            for _ , k in each_null.items():
                null_list.append(k) 
        uncertain_tuple_num += len(results)
        uncertain_quality += sum([math.pow(sensitivity, null_list[k]) for k in range(len(null_list))]) #注意sensitivity必须在(0, 1)之间，不然uncertain_quality>1
    total_num = uncertain_tuple_num+certain_lineage_num
    total_quality = uncertain_quality + certain_lineage_num
    query_quality = total_quality/total_num #必然在[0, 1]之间，不然是错的
    quca_price = uca_price * query_quality * price_coefficient / result_num
    # print(quca_price, uca_price)
    return uca_price, certain_lineage_num, total_completeness, base_price_list, round(query_quality,3), round(quca_price,3)

def get_lineage(sql_statement : str, db):
    table_list,projection_flag, new_sql_statement, where_attributes = parse_sql_statements(sql_statement)
    # get query results on the new_sql_statement
    whole_results = da.select(new_sql_statement, db = db)
    lineage_set = []
    column_name = list(whole_results[0].keys())
    
    df = pd.DataFrame([list(i.values()) for i in whole_results], columns = column_name)
    # remove the tuple with null value
    df.dropna(axis=0, how='any', inplace= True)
    # drop the id column and empty_num columns
    # as they are unseen to the customers/buyers
    remove_columns_list = []
    for each_column in df.columns:
        if('id0' in each_column or 'empty_num' in each_column):
            remove_columns_list.append(each_column)
    df.drop(remove_columns_list, axis = 1, inplace = True) 
    
    
    if projection_flag == 1:
        result_list = []
        # group by the results for projection
        project_columns = list(df.columns[:-2*len(table_list)])
        df_groups = df.groupby(project_columns) # is the dictionary of the groups
        # example:
        # {(1, 2): Int64Index([0, 1], dtype='int64'), (1, 3): Int64Index([2], dtype='int64'), (2, 3): Int64Index([3], dtype='int64')}
        for each_group in df_groups:
            df_each_group = each_group[1] # the dataframes in the same group
            item_index = df_each_group.index[0] # THE first item in the group
            result_list.append(item_index)
        # reserve the projected rows as results 
        # print(result_list)
        df= pd.DataFrame(df.loc[result_list], columns = df.columns) 
    
    # obtain the lineage set
    df_values = np.array(df.values)

    table_num = len(table_list)
    for i in range(table_num):
        lineage_set.append([])
        
        if(-2 * (table_num - i) + 2 == 0):
            lineage_set[i] = np.array(df_values[:,-2 * (table_num - i):])
        else:
            lineage_set[i] = np.array(df_values[:,-2 * (table_num - i):-2 * (table_num - i) + 2])
        
        # remove the duplicated lineages
        lineage_set[i] = list(set([tuple(t) for t in lineage_set[i]]))
        # print("the number of lineage data ", len(lineage_set[i]))
    # drop the assistant columns 
    df.drop(df.columns[-2*table_num:], axis=1, inplace = True)

    return lineage_set, df, where_attributes, table_list

def get_attributes_list(projections_attributes, table_list, db):
    ans_list = []

    # obtain the attributes of all tables        
    tables_attributes_list = {}
    for table_name in table_list:  
        attributes_list = da.select("desc %s"%table_name, db)
        tmp_list = []
        for each_dict in attributes_list:
            tmp_list.append(each_dict['Field'])
        tables_attributes_list[table_name] = tmp_list
        ans_list.append([])
    # check each attribute in the sql query
    for each_attr in projections_attributes:
        if '.' in each_attr:
            temp_str = each_attr.split('.')
            # the value such 0.35 would go to here 
            if(len(temp_str) == 2):
                if(temp_str[0] in table_list and temp_str[1] in tables_attributes_list[temp_str[0]]):
                    ans_list[table_list.index(temp_str[0])].append(temp_str[1])
        else:
            for i in range(len(table_list)):
                if each_attr in tables_attributes_list[table_list[i]]:
                    ans_list[i].append(each_attr)
                    break
    for i in range(len(table_list)):
        # remove the duplicate attributes in each table
        ans_list[i] = list(set(ans_list[i]))
    # print(ans_list)   
    return ans_list

def check_price(buyer_sql, owner, buyer):
    para=da.select("SELECT PriceStrategy From Dataset where Owner = \'%s\'"% owner, 'transaction')
    strategy = para[0]['PriceStrategy']
    no_duplicate_lineage_set, df, where_attributes, table_list= get_lineage(buyer_sql, owner)
    projections_attributes = list(df.columns) + where_attributes
    if(strategy == 'QUCA'):
        para2=da.select("SELECT Pricecoefficient,Sensitivity From User where Name = \'%s\'"% buyer, 'transaction')
        coefficient = float(para2[0]['Pricecoefficient'])
        sensitivity = float(para2[0]['Sensitivity'])
        attributes_list = get_attributes_list(projections_attributes, table_list, owner)
        uca_price, certain_lineage_num, total_completeness, base_price_list, query_quality, quca_price = cal_quca_price(no_duplicate_lineage_set, table_list, attributes_list, sensitivity, coefficient, len(df.values), owner)
    elif(strategy == 'UCA'):
      
        uca_price,certain_lineage_num,total_completeness,base_price_list = cal_uca_price(no_duplicate_lineage_set, table_list, owner)
        query_quality = 1
        quca_price = 0
        coefficient = 0 
        sensitivity = 0

    # 是取UCA还是QUCA会在take order的时候由app.py文件来决定，这边返回所有变量
    
    return uca_price, certain_lineage_num, total_completeness, base_price_list, query_quality, quca_price, coefficient, sensitivity, strategy, df

if __name__ == "__main__":
    #一些测试
    buyer_sql = "select education from Data1 where education between 15 and 20"
    # table_list = parse_sql_statements(buyer_sql)[0]
    # no_duplicate_lineage_set,result, whole_results= get_lineage(buyer_sql, 'yrq')
    # print(cal_uca_price(no_duplicate_lineage_set, table_list, 'yrq'))
    # table_list,split_point,projection_flag, new_sql_statement, projection_attributes = parse_sql_statements(buyer_sql)
    # print(get_attributes('data2','yrq'))
    # print(projection_attributes)
    # print(get_attributes_list(projection_attributes,table_list, 'yrq'))
    # attributes_list = get_attributes_list(projection_attributes, table_list, 'yrq')
    # print(cal_quca_price(no_duplicate_lineage_set, table_list, attributes_list, 0.5, 1, len(whole_results), 'yrq'))
    # print(len(whole_results),len(no_duplicate_lineage_set))
    # print(check_price(buyer_sql,'yrq')) ## 返回9个值，1-4为UCA，5-6为QUCA，7-9为系数
    # a,b,c =[1,2,3]

    # print(parse_sql_statements(buyer_sql))
    print(check_price(buyer_sql,'yrq','lby'))