# 2022-Summer-Research
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

4. delete all files and folders under folder "upload"
5. Run mysql, run the code in file "database_code.txt"
6. Run app.py to start the website
