#!/usr/bin/env python
# coding: utf-8

import json
import pymysql


class Mysql(object):
    # mysql 端口号,注意：必须是int类型
    def __init__(self, host, user, passwd, port, db_name):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.db_name = db_name

    def select(self, sql):
        """
        执行sql命令
        :param sql: sql语句
        :return: 元祖
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.passwd,
                port=self.port,
                database=self.db_name,
                charset='utf8',
                cursorclass=pymysql.cursors.DictCursor
            )
            cur = conn.cursor()  # 创建游标
            # conn.cursor()
            cur.execute(sql)  # 执行sql命令
            res = cur.fetchall()  # 获取执行的返回结果
            cur.close()
            conn.close()
            return res
        except Exception as e:
            print(e)
            return False

    def get_all_db(self):
        """
        获取所有数据库名
        :return: list
        """
        # 排除自带的数据库
        exclude_list = ["sys", "information_schema", "mysql", "performance_schema"]
        sql = "show databases"  # 显示所有数据库
        res = self.select(sql)
        # print(res)
        if not res:  # 判断结果非空
            return False

        db_list = []  # 数据库列表
        for i in res:
            db_name = i['Database']
            # 判断不在排除列表时
            if db_name not in exclude_list:
                db_list.append(db_name)
                # print(db_name)

        if not db_list:
            return False

        return db_list

    def get_user_list(self):
        """
        获取用户列表
        :return: list
        """
        # 排除自带的用户
        exclude_list = ["root", "mysql.sys", "mysql.session"]
        sql = "select User from mysql.user"
        res = self.select(sql)
        # print(res)
        if not res:  # 判断结果非空
            return False

        user_list = []
        for i in res:
            db_name = i['User']
            # 判断不在排除列表时
            if db_name not in exclude_list:
                user_list.append(db_name)

        if not user_list:
            return False

        return user_list

    def get_user_power(self):
        """
        获取用户权限
        :return: {}

        {
            "test":{  # 用户名
                "read":["db1","db2"],  # 只拥有读取权限的数据库
                "all":["db1","db2"],  # 拥有读写权限的数据库
            },
            ...
        }
        """
        info_dict = {}  # 最终结果字典
        # 获取用户列表
        user_list = self.get_user_list()
        if not user_list:
            return False

        # 查询每一个用户的权限
        for user in user_list:
            # print("user",user)
            sql = "show grants for {}@localhost".format(user)
            res = self.select(sql)
            if not res:
                return False

            for i in res:
                key = 'Grants for {}@%'.format(user)
                # print("key",key)
                # 判断key值存在时
                if i.get(key):
                    # print(i[key])
                    # 包含ALL或者SELECT时
                    if "ALL" in i[key] or "SELECT" in i[key]:
                        # print(i[key])
                        if not info_dict.get(user):
                            info_dict[user] = {"read": [], "all": []}

                        cut_str = i[key].split()  # 空格切割
                        # print(cut_str,len(cut_str))
                        power = cut_str[1]  # 权限，比如ALL，SELECT

                        if len(cut_str) == 6:  # 判断切割长度
                            # 去除左边的`
                            tmp_str = cut_str[3].lstrip("`")
                        else:
                            tmp_str = cut_str[4].lstrip("`")

                        # 替换字符串
                        tmp_str = tmp_str.replace('`.*', '')
                        value = tmp_str.replace('\_', '-')

                        # 判断权限为select 时
                        if power.lower() == "select":
                            if value not in info_dict[user].get("read"):
                                # 只读列表
                                info_dict[user]["read"].append(value)
                        else:
                            if value not in info_dict[user].get("all"):
                                # 所有权限列表
                                info_dict[user]["all"].append(value)

        # print(info_dict)
        return info_dict


if __name__ == '__main__':
    host = "localhost"
    user = "root"
    passwd = "root"
    port = 3306
    db_name = "mysql"

    obj = Mysql(host, user, passwd, port, db_name)
    all_db_list = obj.get_all_db()
    user_power = obj.get_user_power()

    print("all_db_list",all_db_list)
    print("user_power",user_power)
