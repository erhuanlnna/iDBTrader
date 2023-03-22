# iDBTrader: Trading SPJ query over incomplete data
1. Download MySQL https://downloads.mysql.com/archives/community/ (we use version 5.7 in this project)
2. Move file "my.ini" into installation directory
3. In file "my.ini", edit basedir and datadir to your installation directory and subdirectory
               ⬇
###################################################
[mysqld]

port=3306

basedir=Your installation directory

datadir=Your installation directory\\data

###################################################

4. In administrator mode, open command prompt, type "(Your installation directory)\bin\mysqld.exe --initialize-insecure"
5. delete all files and folders under folder "upload"
6. Run mysql and the cods in file "database_code.txt" to construct the databases in iDBTrader
7. In file "dataset.py", edit global variable "user" and "pwd" to the username and password of your local database
8. Run app.py to start the website






You can refer to the website (https://www.runoob.com/mysql/mysql-install.html) for easy operation.

