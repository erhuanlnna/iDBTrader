from flask import Flask,render_template, redirect, url_for, flash, request
import dataset
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import os
from utils import query_user, search_all_saledataset, search_order, search_seller_dataset, search_dataset, getSQLPrice, searchALLOrder,searchOrderOfBuyer,searchOrderOfSeller
from datetime import datetime

# 定义price_coefficient 和 sensitivity的默认值, admin可以对其进行修改。
PRICE_COEFFICIENT = 10
SENSITIVITY = 1

#建立一个flask实例
app = Flask(__name__)
#建立一个登陆器实例
login_manager = LoginManager(app)
# secret key 随便设置的
app.secret_key = 'lby'

login_manager.login_view = 'login'
# 设置闪现的错误消息的类别
login_manager.login_message_category = "info"

# 建一个User类用于管理登录
class User(UserMixin):
    pass


# 如果用户名存在则构建一个新的用户类对象，并使用用户名作为ID
# 如果不存在，必须返回None
@login_manager.user_loader
def load_user(username):
    if query_user(username) is not None:
        curr_user = User()
        curr_user.id = username
        return curr_user
    return None

# 登录界面
@app.route("/login",methods=["GET", "POST"])
def login():
    if request.method == 'POST':
        username = request.form.get('Name')
        user = query_user(username)
        # 验证表单中提交的用户名和密码
        if user is not None and request.form['Password'] == user['Password']:
            curr_user = User()
            curr_user.id = username

            # 通过Flask-Login的login_user方法登录用户
            login_user(curr_user)

            # 如果请求中有next参数，则重定向到其指定的地址，
            # 没有next参数，则重定向到"index"视图
            # next = request.args.get('next')
            # return redirect(next or url_for('index'))
            return redirect(url_for('index'))

        flash('Wrong username or password!')
    # GET 请求
    return render_template("login.html")

# 注册界面
@app.route("/register",methods=["POST","GET"])
def register():
    if  request.method == "GET":
        return render_template("register.html")

    #获取表单数据
    result= request.form
    Name = result['Name']
    Password = result['Password']
    Role = request.values.get("Role")
    if Name == '' or Password == '' or Role == None:
        flash("Details should not be empty!")
        return redirect("/register")
    #检测是否重名
    user_info=dataset.select('select * from User where Name = \'%s\'' %Name,'transaction')
    if user_info == ():
        #不重名的话加入数据库中
        dataset.register(Name,Password,Role)
        return redirect("/")
    flash("Name has already existed!")
    return redirect("/register")
    

# 用户主页
@app.route("/",methods=["POST","GET"])
@login_required
def index():
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    if  request.method == "POST":
        keyword = request.form["keyword"]
        did = request.form["did"]
        seller_name = request.form["seller_name"]
        begin_date = request.form["begin_date"]
        end_date = request.form["end_date"]
        search_data = search_dataset(keyword,did,seller_name,begin_date,end_date)
        return render_template("index.html", Role = Role, all_data = search_data)
        
    # Get
    all_data = search_all_saledataset()
    return render_template("index.html", Role = Role, all_data = all_data)

# 上传数据集界面
@app.route("/upload",methods=["POST","GET"])
@login_required
def upload():
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    if Role != "Seller":    ##只有seller可以上传数据集
        return "Access denied!"
    if  request.method == "POST":
        create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        owner = current_user.get_id()
        dname=request.form["name"]
        field=request.form["field"]
        base_price= request.form["base price"]
        if request.form["base price"] != "":
            base_price= float (request.form["base price"])
        keyword=request.form["keyword"]
        price_strategy=request.form["new strategy"]
        file = request.files.get("filename")
        if dname == "" or field == "" or base_price== "" or keyword == "" or price_strategy == "" or file.filename == "":
            flash("Details should not be empty!")
            return redirect(url_for('upload'))
        file_name = file.filename.replace(" ","")
        purename, suffix=os.path.splitext(file_name)
        if suffix != ".csv":
            flash("Wrong data type(only .csv files are accepted)!")
            return redirect(url_for('upload'))
        if not os.path.exists(os.path.dirname(__file__)+'/upload/' + '/%s/'%owner):
            os.makedirs(os.path.dirname(__file__)+'/upload/' + '/%s/'%owner)
        file.save(os.path.dirname(__file__)+'/upload/' + '/%s/'%owner + file_name)  # 保存文件
        #将数据写入用户对应的database里
        size = dataset.write_data(purename,owner,dname)
        # 将数据描述信息存入dataset表里
        insert_sql = '''insert into Dataset(Name, Owner, Field, Size, Keywords, CreateDate, SaleNum,
        State, PriceStrategy, BasePrice, Pricecoefficient, Sensitivity) 
        value(\'%s\',\'%s\',\'%s\',%d, \'%s\',\'%s\', 0 , 1, \'%s\', %.3f,  %.3f, %.3f)'''%(dname,owner,field,size,keyword,create_date,price_strategy,base_price,PRICE_COEFFICIENT,SENSITIVITY)
        insert_db = "transaction"

        dataset.edit(insert_sql, insert_db)

        flash("Upload successfully!")
        return redirect(url_for('upload'))
    # Get
    return render_template("upload.html")

# 修改密码
@app.route("/edit_pwd",methods=["POST","GET"])
@login_required
def edit_pwd():
    if  request.method == "POST":
        entered_pwd=request.form["Old password"]
        new_pwd =request.form["New password"]
        user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
        old_pwd = user_info[0]["Password"]
        if new_pwd != '':    #密码不能为空
            if entered_pwd != old_pwd:
                flash("Wrong old password!")
                return render_template("edit_pwd.html")
            mysql = "update User set Password = \'%s\' where Name = \'%s\'"%(new_pwd,current_user.get_id())
            dataset.edit(mysql,"transaction")
            flash("Password changed!")
            return redirect(url_for('index'))
        flash("Password can't be empty!")
    
    # GET
    return render_template("edit_pwd.html")

# 登出界面
@app.route('/logout')
@login_required
def logout():
    logout_user()
    return 'Logged out successfully!'

# 订单详情界面
@app.route("/order_detail/<OID>/",methods=["POST","GET"])
@login_required
def order_detail(OID):
    # 基本信息获取
    sql = "SELECT * FROM order_table WHERE OID = %s"%OID
    info = dataset.select(sql,"transaction")
    # GET
    return render_template("order_detail.html", info = info)   


#全局变量用来存储用户的sql语句和数据
buyer_sql = ""
buyer_data = {}
price = 0
# 数据集detail界面
@app.route("/detail/<DID>/",methods=["POST","GET"])
@login_required
def detail(DID):
    global buyer_sql,buyer_data, price
    # 基本信息获取
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    sql = "SELECT * FROM Dataset WHERE DID = %s"%DID
    info = dataset.select(sql,"transaction")
    # 不可以访问未售卖的数据集
    if info[0]["State"] == 0:
        flash("Access denied!")
        return redirect(url_for('index'))
    keyword=info[0]["Keywords"].split(';')
    #去找几个sample data
    owner = info[0]["Owner"]
    dname = info[0]["Name"]
    data = dataset.select('select * from %s'%dname,'%s'%owner)
    data = data[0:7]
    for i in range(7):
        del data[i]["id0"]

    # POST
    if  request.method == "POST":
        if request.form.get('checkprice') == 'Check price':
            price = 0
            buyer_sql = request.form["SQL query"]
            try:
                buyer_data = dataset.select(buyer_sql,owner)
            except:
                buyer_sql = ""
                flash("illegal SQL query!")
            else:
                price=getSQLPrice()
                
        if request.form.get('takeorder') == 'Take order' and buyer_sql != "":
            # print(buyer_data)
            DName = dname
            Seller = owner
            Buyer = current_user.get_id()
            Create_Date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            Base_Price = info[0]['BasePrice']
            ############################
            ###########此处待修改########
            Sale_data_num = len(buyer_data)
            Access_data_num = len(buyer_data)
            ############################
            ############################
            Price_coefficient = info[0]['Pricecoefficient']
            Sensitivity = info[0]['Sensitivity']
            Total_completeness = dataset.cal_complete(buyer_data)
            Price_strategy = info[0]['PriceStrategy']
            Price = price
            SQLquery = buyer_sql
            insert_sql = '''insert into order_table(DName, Seller, Buyer, CreateDate, BasePrice, SaleDataNum,
            AccessDataNum, Pricecoefficient, Sensitivity, TotalCompleteness, PriceStrategy, Price, SQLquery) 
            value(\'%s\',\'%s\',\'%s\',\'%s\', %.3f, %d , %d , %.3f, %.3f, %d, \'%s\', %.3f,\'%s\')'''%(DName, Seller, Buyer, Create_Date, Base_Price, Sale_data_num,
            Access_data_num, Price_coefficient, Sensitivity, Total_completeness, Price_strategy, Price, SQLquery)
            dataset.edit(insert_sql, "transaction")

            # 增加售卖次数
            update_sql = "UPDATE Dataset SET SaleNum = \'%s\' WHERE DID = %s "%(info[0]["SaleNum"] + 1,DID)
            dataset.edit(update_sql, "transaction")
            # 重新获取数据
            sql = "SELECT * FROM Dataset WHERE DID = %s"%DID
            info = dataset.select(sql,"transaction")
            # 重置data
            buyer_sql = ""
            buyer_data = {}
            price = 0
        return render_template("detail.html", data=data, keyword=keyword, info = info, price = price, DID=DID, Role = Role) 
    # GET
    buyer_sql = ""
    buyer_data = {}
    price = 0
    return render_template("detail.html", data=data, keyword=keyword, info = info, price = price, DID=DID, Role = Role)   

# 卖家查看自己上传的数据集
@app.route("/manage_dataset",methods=["POST","GET"])
@login_required
def manage_dataset():
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    Name = current_user.get_id()
    if Role == "Buyer":    ##Buyer不可以管理数据集
        flash( "Access denied!" )
        return redirect(url_for('index'))
    if  request.method == "POST":
        keyword = request.form["keyword"]
        did = request.form["did"]
        seller_name = Name
        begin_date = request.form["begin_date"]
        end_date = request.form["end_date"]
        search_data = search_dataset(keyword,did,seller_name,begin_date,end_date)
        return render_template("manage_dataset.html", Name = Name, all_data = search_data)
        
    # Get
    if Role == "Seller":
        all_data = search_seller_dataset(Name)
        return render_template("manage_dataset.html", Name = Name, all_data = all_data)
    else:
        all_data = search_all_saledataset()
        return render_template("manage_dataset.html", Name = Name, all_data = all_data)

# 修改数据集界面界面
@app.route("/edit_dataset/<DID>/",methods=["POST","GET"])
@login_required
def edit_dataset(DID):
    # 基本信息获取
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    sql = "SELECT * FROM Dataset WHERE DID = %s"%DID
    info = dataset.select(sql,"transaction")
    # 禁止修改不是自己的的数据集
    if info[0]["Owner"] != current_user.get_id() and Role != "Admin":
        flash("Access denied!")
        return redirect(url_for('index'))
    keyword=info[0]["Keywords"].split(';')
    # print(info[0]["State"])

    # POST
    if  request.method == "POST":
        sql = ""
        if request.form.get('editname') == 'Change':
            newname = request.form["newname"]
            if newname =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            dataset.edit("rename table %s to  %s"%(info[0]["Name"],newname),info[0]["Owner"]) 
            sql = "UPDATE Dataset SET Name = \'%s\' WHERE DID = %s "%(newname,DID)
        elif  request.form.get('editfield') == 'Change':
            newfield = request.form["newfield"]
            if newfield =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET Field = \'%s\' WHERE DID = %s "%(newfield,DID)
        elif  request.form.get('editbaseprice') == 'Change':
            newbaseprice = request.form["newbaseprice"]
            if newbaseprice =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET Baseprice = \'%s\' WHERE DID = %s "%(newbaseprice,DID)
        elif  request.form.get('editkeywords') == 'Change':
            newkeywords = request.form["newkeywords"]
            if newkeywords =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET Keywords = \'%s\' WHERE DID = %s "%(newkeywords,DID)
        elif  request.form.get('editstate') == 'Sell/Unsell':
            newstate = info[0]["State"]
            if newstate == 1:
                newstate = 0
            elif newstate == 0:
                newstate = 1
            sql = "UPDATE Dataset SET State = \'%s\' WHERE DID = %s "%(newstate,DID)
        elif  request.form.get('editstrategy') == 'Change':
            newpricestrategy = request.form["newstrategy"]
            if newpricestrategy =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET PriceStrategy = \'%s\' WHERE DID = %s "%(newpricestrategy,DID)
        elif  request.form.get('editpricecoefficient') == 'Change':
            newpricecoefficient = request.form["newpricecoefficient"]
            if newpricecoefficient =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET Pricecoefficient = \'%s\' WHERE DID = %s "%(newpricecoefficient,DID)
        elif  request.form.get('editsensitivity') == 'Change':
            newsensitivity = request.form["newsensitivity"]
            if newsensitivity =="":
                return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)
            sql = "UPDATE Dataset SET Sensitivity = \'%s\' WHERE DID = %s "%(newsensitivity,DID)
        
        dataset.edit(sql,"transaction") 
        info = dataset.select("SELECT * FROM Dataset WHERE DID = %s"%DID,"transaction")
        keyword=info[0]["Keywords"].split(';')
         
    # GET
    return render_template("edit_dataset.html", Role=Role,keyword=keyword, info = info, DID=DID)



# 管理订单
@app.route("/order_management",methods=["POST","GET"])
@login_required
def order_management():
    user_info = dataset.select('select * from User where Name = \'%s\'' %current_user.get_id(),'transaction')
    Role = user_info[0]["Role"]
    if  request.method == "POST":
        Dataset_name = request.form["DName"]
        Order_No = request.form["OID"]
        Seller_name = ""
        Buyer_name = ""
        if Role != "Seller":
            Seller_name = request.form["Seller"]
        if Role != "Buyer":
            Buyer_name = request.form["Buyer"]
        begin_date = request.form["begin_date"]
        end_date = request.form["end_date"]
        search_data = search_order(Dataset_name,Order_No, Buyer_name, Seller_name, begin_date, end_date)
        return render_template("order_management.html", Role = Role, all_data = search_data)
        
    # Get
    if Role == "Admin":
        all_data = searchALLOrder()
    elif Role == "Seller":
        all_data = searchOrderOfSeller(current_user.get_id())
    else:
        all_data = searchOrderOfBuyer(current_user.get_id())
    return render_template("order_management.html", Role = Role, all_data = all_data)

if __name__ == "__main__":
    app.run()
