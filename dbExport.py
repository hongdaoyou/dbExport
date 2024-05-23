#!/usr/bin/env python36

import xlrd
import pymysql
import threading
import time

dbAddr = '192.168.1.130'  #数据库地址
dbUser = 'root' #用户名
dbPasswd = 'hdy' #密码
dbName = 'dd' #数据库
tableName = 'test1'#表名
excelName = 'a.xlsx'; #excel路径

# 以上,请配置
#####

oneUnitNum = 50000; # 50000  # 多少条记录启用1个线程
oneSqlNum = 100 # 1条sql语句,最多插入多少条


threadIdData = [] # 线程的数组
excelData=[];  # 存储的数据

# lastLineFlag 1:最后一条
# 读取50000条,就起一个线程去处理数据库插入操作
def save_excel_data(lineData, lastLineFlag):
    global oneUnitNum;
    global excelData
    excelData.append(lineData )

    if lastLineFlag == 1 or len(excelData) >= oneUnitNum : #最后1条,或是50000的倍数
        oneExcelData = excelData.copy()
        #insert_db(oneExcelData)
        #threadId =_thread.start_new_thread(insert_db, (oneExcelData ,))
        threadObj = mythread(oneExcelData )
        threadObj.start();
        threadIdData.append(threadObj )
        excelData = []#重置数据

# 读execl
def read_from_excel(excelName):
    # 打开execl
    workSheet = xlrd.open_workbook(excelName)
    
    # 读取,指定的表格
    sheet1 = workSheet.sheet_by_index(0 )
    nrows = sheet1.nrows #行数
    ncols = sheet1.ncols #列数

    # 读取,所有的行
    for line in range(0, nrows):
        lineData = [] #一行的数据
        
        # 读取,每行的内容
        for value in sheet1.row_values(line):
            lineData.append(str(value))

        lastLineFlag = nrows == line + 1; #是否最后1条
        
        # 存入
        save_excel_data(lineData, lastLineFlag )
    #print(excelData )


#插入数据库
def insert_db(excelData):
    
    # 连接
    dbConn = pymysql.connect(dbAddr, dbUser, dbPasswd, dbName)
    if not dbConn:
        print('db connect failed')
        exit(1)

    # 创建,游标
    cursor = dbConn.cursor()
    
    totalNum = len(excelData );
    global oneSqlNum; 
    valueSql = '';
    #插入数据
    mysql = "insert into " + tableName + " value ";
    
    # 进行,拼接
    for i in range(1, totalNum + 1):
        if valueSql:
            valueSql += ",";
        valueSql += "( '" + "','".join(excelData[i - 1] ) +  "' )"
        
        if   totalNum == i or i % oneSqlNum == 0 : # 最后1条, 或者条数是100的倍数
            mysqlTotal = mysql + valueSql
            print(mysqlTotal )
            # print(valueSql )
            cursor.execute(mysqlTotal)
            dbConn.commit()
            valueSql = '' # 清空之前积累的value

#线程类: 插入数据库操作
class mythread(threading.Thread):
    def __init__(self, excelData):
        threading.Thread.__init__(self) 
        self.excelData = excelData

    def run(self):
        print('one threading start')
        insert_db(self.excelData)
        print('one threading end')


###############
#runing
###############

#read from excel
read_from_excel(excelName )

# join threading
for value in threadIdData:
    value.join()

print('over')
