# todolist 提取其他信息到基本信息，便于搜索整理。
# v0.11 对多文件数据提取，以数据库形式保存到MySQL，包含(Name,Gender,Birthday,Hoku,Email,Mobile)等信息。
#      引入time耗时监测修饰器；提高匹配命中，改变提取内容方法为单循环搜索（get_basic_info函数），或生成器方法。
#      解决插入表名%s问题，采用字符串拼接；引入sys中对非BMP码的替换容错；数据文件编码增加容错errors='ignore'；引入conn.rollback()，事务原子性。
# v0.1  对单文件内容进行for循环读取每条信息进行操作。
# v0.01 对单文件内单条内容以数据库形式保存到MySQL。


from bs4 import BeautifulSoup
import os
import MySQLdb

import time  # 用于检验耗时，可屏蔽.(单文件约耗时12秒。)
import functools  # 用于计时器修饰器

import sys  # 该部分用于对Basic Multilingual Plane之外的解码信息进行编码替换

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)  # 同上


def calc_time(func):  # 用于检验耗时修饰器
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.clock()
        result = func(*args, **kwargs)
        end = time.clock()
        print('单个文件内信息整理写入，%s函数运行耗时%.3f秒' % (func.__name__, (end - start)))
        return result

    return wrapper


'''
def get_single_info(person_basic_info,value_searched):
    i = 0
    j = 0
    for single_info in person_basic_info:
        #print(i)
        i += 1
        if single_info.get_text().find(value_searched[j])!= -1:
            #print(single_info.get_text())
            #print(i)
            j += 1
            yield person_basic_info[i].get_text()

def get_basic_info(intrd_each):
    classified_data = BeautifulSoup(intrd_each,'lxml')
    #print(classified_data.prettify())
    person_basic_info = classified_data.findAll('table')[1].findAll('td')
    #print(len(person_basic_info))
    person_basic_info_list = []
    if (len(person_basic_info)>=14):
        value_searched = ['名：','别：','期：','口：','件：','话：']
        for each_info in get_single_info(person_basic_info,value_searched):
            person_basic_info_list.append(each_info)
        print(person_basic_info_list)
        #person_basic_info_list.append(classified_data.prettify())
    return person_basic_info_list
'''


def get_basic_info(intrd_each):
    classified_data = BeautifulSoup(intrd_each, 'lxml')
    # print(classified_data.prettify())
    person_basic_info = classified_data.findAll('table')[1].findAll('td')  # 根据文件特征提出数据段。
    # print(len(person_basic_info))

    person_basic_info_list = []
    if (len(person_basic_info) >= 14):  # 对有效信息，只遍历一遍依次提取信息。找到前一个后，才从该位置继续找后一个。
        value_searched = ['名：', '别：', '期：', '口：', '件：', '话：']
        j = 0
        for i in range(len(person_basic_info)):
            if (person_basic_info[i].get_text().find(value_searched[j]) != -1) and (
                len(person_basic_info[i].get_text().strip()) < 10):  # j>=6时后面肯定报错！有的td段很长,且为无关信息增加筛选。
                person_basic_info_list.append(person_basic_info[i + 1].get_text().strip())
                if (j == 5):
                    break
                j += 1
        # print(person_basic_info_list)

        temp_list_len = len(person_basic_info_list)
        if temp_list_len != 6:  # 如果匹配数据项未找齐，特别是最后一项，则以''补齐。
            for i in range(6 - temp_list_len):
                person_basic_info_list.append('')
            print(person_basic_info_list)

        person_basic_info_list.append(classified_data.prettify().translate(
            non_bmp_map))  # ！！替换不识别，数据编码中'UCS-2' codec can't encode characters，Non-BMP character not supported in Tk问题。
    return person_basic_info_list


def open_db_creat_table(database_name, table_name):
    conn = MySQLdb.connect(host='localhost', user='root', passwd='', db='', charset='utf8')
    cursor = conn.cursor()
    ##cursor.execute('DROP DATABASE IF EXISTS %s' % database_name) #测试时使用！！谨慎！
    cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % database_name)
    cursor.execute('USE %s' % database_name)

    sql_create_table = """
    CREATE TABLE IF NOT EXISTS %s (
    Name NVARCHAR(32),
    Gender NVARCHAR(8),
    Birthday NVARCHAR(32),
    Hoku NVARCHAR(50),
    Email NVARCHAR(50),
    Mobile NVARCHAR(32),
    Resume TEXT(45000),
    id INT(20) NOT NULL AUTO_INCREMENT,
    PRIMARY KEY(id)
    );
    """
    cursor.execute(sql_create_table % table_name)
    return conn, cursor


def insert_table(table_name, person_list, conn, cursor):
    try:
        sql_insert = 'INSERT INTO ' + table_name + '(Name,Gender,Birthday,Hoku,Email,Mobile,Resume) VALUES (%s,%s,%s,%s,%s,%s,%s);'  # insert into 后要加空格！

        # 使用%s方法同时格式化字符串中表名和值时表名格式化总报错，改用拼接字符串。

        # for i in person_list: #该段语句用于检测插入数据报错位置，将下段语句屏蔽后使用。
        #    print(i[0])
        #    cursor.execute(sql_insert,i)

        cursor.executemany(sql_insert, person_list)

        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    else:
        print('成功将该文件内容信息插入SQL库。')


def query_table(table_name, conn, cursor):  # 抽取or检验数据是否完全插入没有错误缺失。
    cursor.execute("SELECT Name,Gender,Birthday,Hoku,Email,Mobile FROM %s" % table_name)
    person_cds = cursor.fetchall()
    # for each in cds:
    #    print(each)
    print('SQL库内查询共%s条信息。' % len(person_cds))


@calc_time  # 计时器修饰，可屏蔽。
def save_file_sql(file_name, table_name, conn, cursor):
    person_list = []
    num = 0

    with open(file_name, 'rt', encoding='GB18030', errors='ignore') as raw_data:  # 增加解码error容错。
        print('正在提取文件%s中信息' % file_name)
        for intrd_each in raw_data:
            person_basic_info_list = get_basic_info(intrd_each)
            if person_basic_info_list != []:
                person_list.append(person_basic_info_list)
                num += 1
    print('文件内容查询，提取%s条信息。' % num)
    # for i in person_list:
    #    print(i)
    insert_table(table_name, person_list, conn, cursor)


def save_info_sql_main():
    read_dir = r'G:\*\*\*'
    os.chdir(read_dir)

    database_name = 'person_info'
    table_name = 'basic_info'
    conn, cursor = open_db_creat_table(database_name, table_name)

    # file_name = '10000000.data' #后续补充插入时使用，将下段语句及database删除语句屏蔽。
    # save_file_sql(file_name,table_name,conn,cursor)

    for i in range(0, 1):
        file_name = '%s.data' % (10000000 + 500 * i)
        save_file_sql(file_name, table_name, conn, cursor)

    query_table(table_name, conn, cursor)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    save_info_sql_main()

