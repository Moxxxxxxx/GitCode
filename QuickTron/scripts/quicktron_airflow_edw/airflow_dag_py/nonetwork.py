#!/usr/bin/env python3
# coding=utf-8


import sys
import os
import pymysql
import subprocess
import time
from datetime import datetime, timedelta


class NoNetwork(object):


    def __init__(self):
        """
        初始化实例属性，接受传入的参数变量,进行初始化
        :return: 对类属性进行实例化
        """
        self.param = sys.argv
        self.file_name_list = list() # 文件的目录
        self.file_name_zip_set = set() # 文件压缩包的目录
        self.conn = pymysql.connect(host="008.bg.qkt", port=3306, user="root", password="quicktron123456", database="collection_offline", charset="utf8", autocommit=True)
        self.cursor = self.conn.cursor()
        self.pre1_date = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
        self.pre7_date = (datetime.now() + timedelta(days=-7)).strftime("%Y-%m-%d")

    def unzipFile(self, zip_file_list):
        """
        进行压缩包的解压缩操作
        :return:
        """

        unzip_file_pre1_date = self.param[2] + self.pre1_date
        unzip_file_pre7_date = self.param[2] + self.pre7_date

        if os.path.exists(unzip_file_pre1_date):
            subprocess.call(["rm", "-rf", unzip_file_pre1_date])
        if os.path.exists(unzip_file_pre7_date):
            subprocess.call(["rm", "-rf", unzip_file_pre7_date])


        for zip_file in zip_file_list:
            exec_code = subprocess.call(["unzip", "-o", zip_file, "-d", unzip_file_pre1_date])
            if exec_code == 0:
                print("======================解压文件%s完成" % zip_file)

        print("======================全部的待解压文件包已全部解压完成")

    def rmZipAndUnzipFile(self):
        """
        清除昨天的解压缩的文件目录，以及移动七天之前的压缩文件目录到指定目录
        :return:
        """
        # mv -b /opt/docker/offline/2022-11-26/ /opt/docker/offline-bak/
        # rm -rf /opt/docker/offline-txt/2022-11-28/

        # 判断是否七天之前的压缩包路径是否还存在
        pre7_dir_path = "{0}{1}/".format(self.param[1], self.pre7_date)
        pre7_target_dir_path = "{0}{1}/".format(self.param[3], self.pre7_date)
        if os.path.exists(pre7_dir_path):
            if os.path.exists(pre7_target_dir_path):
                subprocess.call(["rm", "-rf", pre7_target_dir_path])
            reode = subprocess.call(["mv", "-b", pre7_dir_path, self.param[3]])
            if reode == 0:
                print("======================成功备份%s压缩包的数据归档" % pre7_dir_path)
            else:
                raise Exception("备份七天之前压缩包归档出现问题")
        else:
            print("======================不存在七天之前压缩包，无需进行数据备份归档")

        pre1_dir_path = "{0}{1}/".format(self.param[2], self.pre1_date)
        if os.path.exists(pre1_dir_path):
            subprocess.call(["rm", "-rf", pre1_dir_path])
            print("======================成功清除%s解压缩的文件目录" % pre1_dir_path)
        else:
            print("======================不存在昨日的解压缩文件目录，不需要进行清除")





    def getFileListName(self, path_name, collections, is_set):

        """
        递归读取目录的所有文件全路径(同一个项目的，放在一个key中)
        :param path_name:文件全路径名称
        :return:map{string,list} key: project_code value: 文件路径
        """
        list_dir = os.listdir(path_name)
        for flie_dir in list_dir:
            com_dir = os.path.join(path_name, flie_dir)
            if os.path.isdir(com_dir):
                self.getFileListName(com_dir, collections, is_set)
            elif os.path.isfile(com_dir):
                if is_set == 1:
                    zip_filename = os.path.basename(com_dir)
                    collections.add(zip_filename.split("-")[0])
                else:
                    collections.append(com_dir)


        return collections


    def getDBProjectBol(self, project_code):
        """
        判断是否数据库存在此项目，若存在返回True，不存在返False
        :param project_code: 项目编号
        :return: Bool
        """
        flag = False
        project_sql = "select count(1) from collection_offline.collection_project_record_info where project_code=%s and is_nonetwork = '1'"
        self.cursor.execute(project_sql, (project_code))

        one_result=  self.cursor.fetchone()
        if one_result[0] == 1:
            flag = True

        return flag


    def readerDBsourceTable(self):
        """
        读取2.x的需要的表的字段定义()
        :return: map{map} -> leve1: key: 库名.表名 value：map  leve2: key:字段名  value:字段的类型
        """
        map1 = dict()
        list1 = list()

        source_table_sql = "select database_name,table_name,is_full from collection_offline.collection_table_info"

        self.cursor.execute(source_table_sql)

        for table in self.cursor.fetchall():
            map2 = dict()
            db_table_name = table[0] + "." + table[1]
            self.cursor.execute("desc " + db_table_name)
            for column in self.cursor.fetchall():
                map2[column[0]] = column[1]
            map1[db_table_name] = map2
            if int(table[2]) == 1:
                list1.append(db_table_name)
        return map1, list1


    def DBThan(self, file_list_copy, db_map):
        """
        数据库进行比对
        :param file_list_copy  文件列表
        :param db_map 源数据库字典
        :return:
        """

        alter_list_sql = list()

        for file in file_list_copy:
            # 获取库名，表名
            dir_list = file.split(os.path.sep) # 切分路径

            dir_list_dbname = dir_list[-3] # 库名
            dir_list_tbname = dir_list[-2] # 表名

            db_tb_name = dir_list_dbname + "." + dir_list_tbname

            with open(file=file, mode="r", encoding="utf-8") as fb:

                headStr_list = fb.readline().split("=")

                columns_list = headStr_list[0].split(",")
                column_types_list = headStr_list[2].split("-")
                i = 0
                if db_tb_name in db_map.keys():

                    map1_value = db_map.get(db_tb_name)

                    while i < len(headStr_list):
                        if columns_list[i] not in map1_value.keys():
                            alter_list_sql.append("alter table %s add column %s %s;" % (db_tb_name, columns_list[i], column_types_list[i]))
                        i += 1
        print("======================进行数据库对比结束")
        return alter_list_sql









    def main(self):
        # 获取压缩包的目录的所有的项目名称
        """
        主执行函数

        :return:
        """

        if len(self.param) != 4:
            raise Exception("参数个数不对，参数必须为三个，第一个参数为压缩包的路径，第二个参数为解压过后的压缩包的路径，第三个参数为压缩包的备份路径")
        else:
            # 判断前一天的压缩的包的路径是否存在
            zip_dir = self.param[1] + self.pre1_date
            unzip_dir = self.param[2] + self.pre1_date
            if not os.path.exists(zip_dir):
                # print("======================今日无压缩包，无需进行下一步操作======================")
                return 1

            print("======================init start")

            zip_file_dir_list = self.getFileListName(path_name=zip_dir, collections=list(), is_set=0)
            self.unzipFile(zip_file_dir_list)  # 解压缩文件


            # 获取压缩包的所有项目编码 set()
            project_set = self.getFileListName(path_name=zip_dir, collections=self.file_name_zip_set, is_set=1)

            # 获取表字段描述，以及需要全量抽取的表
            table_column_desc, table_full_list = self.readerDBsourceTable()

            # 获取解压过后的文件路径
            file_list = self.getFileListName(path_name=unzip_dir, collections=self.file_name_list, is_set=0)

            if len(project_set) > 0:
                # 变量数据 获取需要进行比对的project_code的集合
                for project_code in project_set.copy():
                    if self.getDBProjectBol(project_code):
                        project_set.remove(project_code)

            print("======================获取需要比对的项目编码结束")

            # 判断项目编码集合是否为空 不为空进行字段比对
            if len(project_set) > 0:

                file_list_copy = list() # 定义需要进行字段比对的数据目录

                for every_project in project_set:
                    for file in file_list:
                        if every_project in file:
                            file_list_copy.append(file)

                print("======================获取需要比对的项目文件结束")


                # 进行数据库对比，比进行数据修改操作

                alter_sql_list = self.DBThan(file_list_copy, table_column_desc) # 返回需要修改的sql列表

                if len(alter_sql_list) > 0:
                    # 进行数据操作增加字段
                    for sql in alter_sql_list:
                        self.cursor.execute(sql)
                        print("======================执行修改的语句为：%s" % sql)

                else:
                    print("======================无项目需要修改数据库表字表操作")





        self.cursor.close()
        self.conn.close()
        print("======================init end")

        print("======================start-txt-to-db")
        time.sleep(5)  # sleep 5s

        subprocess.call(["sh", "/opt/docker/offline-script/start_txt_json.sh", self.param[2] + self.pre1_date])


        print("=======================end-txt-to-db")



        print("======================start bak and rm")

        self.rmZipAndUnzipFile()

        print("======================end bak and rm")
        return 0



if __name__ == '__main__':
    t = NoNetwork()
    print(t.main())
