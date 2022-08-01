
"""

对数据库本身的修改：
1. 新增orderTable表：由于一个order里面涉及多个table，增加一个表来表示这种一对多的关系；包含三个属性，order_id, table_name, base_price
2. Order表：原本的Base Price名称改为UCA_price；把DName属性改成sql_statement，即sql语句，注意sql语句可能很长，需要把字符串长度设置的大一些（好像其实已经有了？）；增加一列query_quality属性(float)
3. 对于用户上传的每个数据表，在数据库里copy过去后，增加1 列completeness（即每条数据的完整性，对应非空field个数/总的field个数），这个可以用触发器实现；也可以不做，那代码里就麻烦一些，每次都我们自己查
4. User增加一列属性sensitivity、一列price_coefficient、一列price_strategy（即Database表中的sensitivity、price_coefficient、price_strategy是user的属性，sensitivity默认为0.5，price_coefficient默认为1，price_strategy设为UCA）
这里这样做是因为如果把这3个参数设置成table级别的，处理起来不对劲，每个table的定价方式都不一样，实际这些是数据库级别的，现在一个seller一个数据库，所以这些属性设置成user级别的

展示部分代码的修改：
1. Order表：由于一个order里面涉及多个table；所以展示的时候得把所有table的name和basePrice展示出来，原来只用了select * from order where id = 1;现在还需要执行select * from orderTable where id = 1;吧多个表的名称和BasePrice一起展示出来
2. 管理员上线可以看到User表，并修改sensitivity、price_coefficient, 类似看到数据集那样
3. 每个seller可以修改自己price_strategy（而不是针对特定的表，针对所有的表，因为现在的数据集展示页面需要修改）
"""
import pickle
import datetime
import math
import os
import pandas as pd
# parse the sql statements 
def parse_sql_statements(sql_statement : str): # use regex expressions
	# 1. split the string based on "select"/"from"/"where"
	# 2. Obtain the tables' name by splitting the string between "from" and "where"
	# 3. Obtain the distinct projection attributes by splitting the string between "select" and "from"

	# some example codes:https://blog.csdn.net/u011640418/article/details/119550127
	# https://xiaoshuwen.blog.csdn.net/article/details/106258183
	import re
	rule_projections = r'select(.*?)from'
	projections = re.findall(rule_projections, sql_statement)
	projections.replace(' ', '') # remove blank spaces
	projections_attributes = projections.split(',') # get projection attributes
	output_attributes = projections.split(',')
	# is projections_attributes distinct? ....
	
	# other codes are similar
	
	
	# obtain a string new_sql_statement that 
	# 1) removing the "distinct" string
	# 2) adding the table ids between "select" and "from"
	
	#构建一个attribute_list，二维列表，length和table_list相同，包含每个table上sql语句使用的attribute；所以这里还得想办法知道每个table有哪些属性
	# 例如：select name, age, dept_place from students, department where students.dept_name = department.dept_name
	# table_list:[students, department]
	# attribute_list:[[name, age, dept_name], [dept_place, dept_name]]
	# output_attributes: [name, age, dept_place]
	# distinct_attributes: [] (空)
	# 注意如果distinct_attributes不为空，那么output_attributes与distinct_attributes相同，因为distinct关键字只能放在开头
	
	# return the list of tables,  the distinct attributes, the new_sql_statement, all involved attributes, output_attributes
	return table_list, distinct_attributes, new_sql_statement, attribute_list, output_attributes
	
	
# obtain the lineage sets of the new_sql_statement that has no projections (no "distinct") and selects table ids in table_list,  e.g., select a.info, a.id, b.id from a, b where a.id = b.aid and a.price < 10
def lineage(output_attributes : list, new_sql_statement : str,  table_list : list, distinct_attributes : list):
	
	# execute the new_sql_statement 
	# do this by connectting the databases and getting results (denoted as "whole_results")
	
	
	# the tuples to construct each data in "whole_results" form one lineage
	# each data actually forms one lineage
	lineage_set = []
	sql_results = []
	for each_data in whole_results:
		if(None not in each_data.values()): # 数据中没有null(/None)属性
			# the tables' id are the last len(table_list) elements
			tables_id = each_data[len(each_data)-len(table_list):]
			lineage_set.append(tables_id)
			sql_results += each_data[:len(output_attributes)]
		else:
			whole_results.remove(each_data) #若有null，这条数据不是查询结果，去掉
	
	if(len(distinct_attributes) == 0): # no projection
		return lineage_set, whole_results, sql_results
	else:
		whole_results.sort() # 注意此时的whole_results 是没有null或None属性的
		last_data = []
		current_data = []
		projection_lineage_set = []
		projection_resutls = [] # 生成不含空的projection结果
		for each_data in whole_results:
			# distinct attributes
			current_data = each_data[:len(distinct_attributes)]
			if(current_data == last_data):
				#忽略相同元组的lineage
				continue
			else:
				projection_resutls += current_data
				tables_id = each_data[len(each_data)-len(table_list):]
				projection_lineage_set.append(tables_id) # the table ids 
			last_data = current_data
		return projection_lineage_set, whole_results, projection_resutls


	

def cal_uca_price(no_duplicate_lineage_set : list, table_list:list):

	price = 0
	certain_lineage_num = 0
	total_completeness = 0
	base_price_list = []
	for i in range(len(no_duplicate_lineage_set)):
		base_price = f"select baseprice from dataset_table where table_name = '{table_list[i]}'"
		base_price_list += [base_price]
		for each_lineage in no_duplicate_lineage_set[i]:
			# each_lineage对应一个id
			# 获取这个id所在表的base price和这个id对应数据的completeness
			# 这里一条条执行sql可能很慢，所以可以在外面执行一整个sql语句，然后按照each_lineage对应的id选择
			#如：select id, isnull(name)+isnull(gender)+isnull(age) as score from {table_list[i]}
			# 如果数据库可以修改的话，也可以给每个表单独增加一个列（使用触发器实现）completeness，进行完整度的自动计算

			empty_field_num = f"select isnull(name)+isnull(gender)+isnull(age) as score from {table_list[i]} where id = {each_lineage}"
			# 上面两个是执行对应语句后的记过
			completeness = 1-/attribute_num #完整度=1-空field个数/总的field个数
			total_completeness += completeness
			price += base_price * completeness
			certain_lineage_num += 1
		
	
	return  price, certain_lineage_num, total_completeness, base_price_list
	

	
def cal_quca_price(no_duplicate_lineage_set : list, table_list:list, attribute_list: list, sensitivity_list: float, price_coefficient: float, result_num, int): 
	# result_num 是whole_results的大小, 即len(whole_results)
	uca_price,certain_lineage_num,total_completeness,base_price_list = cal_uca_price(lineage_set, table_list)
	# 计算查询质量，需要计算在attribute_list上为空的元素
	uncertain_tuple_num = 0
	uncertain_quality = 0
	#uncertain_lineage_set = []
	for i in range(len(attribute_list)):
		uncertain_lineage_set.append([])
		table_name = table_list[i]
		is_null_sql = f''
		for j in range(len((attribute_list[i])))
			is_null_sql += f'isnull({attribute_list[i][j]})+'			
		is_null_sql.strip('+') #去掉最后一个加号
		
		#例子：select ID, isnull(dept_name),isnull(age) from student;
		results = f"select id, {is_null_sql} from {table_name} where {is_null_sql}<>0}"
		# results 对应论文中的uncertain lineage
		results = list(results.values()) # 不需要dict中的key
		#uncertain_lineage_set[i] += [results[k][0] for k in range(len(results))]
		uncertain_tuple_num += len(results)
		uncertain_quality += sum([math.exp(sensitivity, results[k][1]) for k in range(len(results))]) #注意sensitivity必须在(0, 1)之间，不然uncertain_quality>1
		
	
	total_num = uncertain_tuple_num+certain_lineage_num
	total_quality = uncertain_quality + certain_lineage_num
	query_quality = total_quality/total_num #必然在[0, 1]之间，不然是错的
	quca_price = uca_price * query_quality * price_coefficient / result_num
	
	return uca_price，certain_lineage_num, total_completeness, base_price_list, query_quality, quca_price


def cal_price(seller : str, buyer : str, sql_statement :str):
	"""seller是对应的卖家，如果id是str类型就是str，否则就是对应的id类型；
	buyer同理，sql_statement是输入的sql语句
	"""
	#执行sql语句获得price_strategy，可以为uca, quca, h-uca, h-quca
	price_strategy = f'select price_strategy from user where user.id = {seller}'
	# 读取sensitivity、price_coefficient的值（从数据库中）
	sensitivity, price_coefficient = f'select sensitivity, price_coefficient from user where user.id = {buyer}'
	# 执行语句得到sensitivity、price_coefficient
	
	#解析语句
	table_list, distinct_attributes, new_sql_statement, attribute_list, output_attributes = parse_sql_statements(sql_statement)
	if(len(output_attributes) == 0): # empty outputs
		return 0, None #告知前端没有sql结果
	
	# calculate lineage
	lineage_set, whole_results, sql_results = lineage(output_attributes, new_sql_statement,  table_list, distinct_attributes)
	if(len(lineage_set) == 0 or len(sql_results) == 0):
		return 0, None # empty outputs #告知前端没有sql结果
	
	# lineage_set是一个二维列表，每一行是一个tuple的lineage，每一列是一个table中属于lineage的id
	# 对lineage_set进行去重，即每一列上没有重复元素
	# 下面的代码就是用双重循环访问lineage_set，为每一列构建一个set进行去重
	no_duplicate_lineage_set = [set([lineage_set[j][i] for j in range(len(lineage_set))]) for i in range(lineage_set[0])]
	
	if(price_strategy.find('h') != -1): # history-aware pricing strategy
		# remove the bought lineage from files
		counter = 0
		for table_name in table_list:
			file_name = f' {seller}-{buyer}-{table_name}.txt'
			if(os.path.exists(file_name)): #文件存在
				with open(file_name, 'rb') as f:
					my_list = pickle.load(f)
				f.close()
				bought_lineage_on_this_table = set(my_list) 
				if(len(bought_lineage_on_this_table) == 0):
					counter += 1
					continue
				else:
					# 两个set做减法
					lineage_on_this_table = no_duplicate_lineage_set[counter]
					lineage_on_this_table = lineage_on_this_table - bought_lineage_on_this_table
					no_duplicate_lineage_set[counter] = lineage_on_this_table
					counter += 1
			else:
				counter += 1
				continue
	# now cal_price()
	result_num = len(sql_results)
	if(price_strategy.find('quca') != -1): #quca_price
		uca_price, certain_lineage_num, total_completeness, base_price_list =cal_uca_price(no_duplicate_lineage_set, table_list)
		query_quality = 1
		final_price = uca_price
	else: # uca_price
		uca_price，certain_lineage_num, total_completeness, base_price_list, query_quality, final_price = cal_quca_price(no_duplicate_lineage_set, table_list, attribute_list, sensitivity, price_coefficient, result_num)
	
	# 这个函数返回后前端显示价格final_price，其他用于下单后数据库的更新
	return final_price, uca_price，certain_lineage_num, total_completeness, base_price_list, query_quality, result_num, no_duplicate_lineage_set, sql_results, sensitivity, price_coefficient,price_strategy, output_attributes

#除了前三个输入，其余都是cal_price的输出
def take_order(seller : str, buyer : str, sql_statement :str, final_price: float, uca_price: float，certain_lineage_num: int, total_completeness: float, base_price_list:list, query_quality: float, result_num: int, no_duplicate_lineage_set: list, table_list: list, sql_results: list, sensitivity: float, price_coefficient: float,price_strategy:str, output_attributes: list):
	# update order数据库
	import datetime
	create_date = str(datetime.date.today())
	update_sql = f'insert into order values("{sql_statement}", {seller}, {buyer}, "{create_date}", {uca_price}, {result_num}, {certain_lineage_num}, {price_coefficient}, {sensitivity}, {total_completeness}, "{price_strategy}", {final_price}, {query_quality})'
	# 执行语句并得到order_id
	
	# update ordertable 数据库
	table_sql = ""
	for i in range(len(table_list)):
		table_name = table_list[i]
		base_price = base_price_list[i]
		update_sql = f'insert into ordertable values({order_id}, {table_name}，{base_price})'
		#执行语句
	
	#将no_duplicate_lineage_set写入文件中
	counter = 0
	for table_name in table_list: #和之前的lineage合并，重写文件
		file_name = f' {seller}-{buyer}-{table_name}.txt'
		with open(file_name, 'rb') as f:
			bought_lineage_on_this_table = pickle.load(f)
		bought_lineage_on_this_table += list(no_duplicate_lineage_set[counter])
		with open(file_name, 'wb') as f:
			pickle.dump(bought_lineage_on_this_table, f)
		f.close()
		counter += 1
	
	# 生成sql_results的csv发给buyer（前端有下载该文件的按钮）
	data = pd.DataFrame(sql_results, columns=output_attributes)
	results_name = f"{order_id}-sql-results.csv"
	data.to_csv(results_name,line_terminator="\n")
	# 此时已经下单成功，前端提示完成下单，提供文件下载按钮即可
	return order_id
		
		
	
	
	
	
	
		
				
			
			
			
	
		
	
	
	
	

	
		
	
	
	
