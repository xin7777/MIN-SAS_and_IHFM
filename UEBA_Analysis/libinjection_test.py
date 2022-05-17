import libinjection

import unittest


# LibInjection单元测试类
class TestLibInjection(unittest.TestCase):

    # def test_sqli(self):
    #     res = libinjection.is_sql_injection("/login?name=admin&pass=admin' and 1=1")
    #     self.assertTrue(res['is_sqli'])

    def test_sqli2(self):
        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and 1=1 and 'a'='a")
        self.assertTrue(res['is_sqli'])

    # 测试猜解数据库表名
    def test_sqli3(self):
        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (select "
                                            "count(*) from data)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

    # 猜解数据库字段名
    def test_sqli4(self):
        res = libinjection.is_sql_injection("/login?pass=admin&name=admin'and (select count("
                                            "uname) from data)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin'and (select count("
                                            "upass) from data)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

    # 猜解密码长度
    def test_sqli5(self):
        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)>1)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)<10)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)<10)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)<5)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)>8)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection("/login?pass=admin&name=admin' and (Select "
                                            "count(*) from data where uname='wucm' and len(upass)=6)>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

    # 猜解密码
    def test_sqli6(self):
        res = libinjection.is_sql_injection(
            "/login?pass=admin&name=admin' and (Select count(*) from data where uname='wucm' "
            "and mid(upass,1,1)<'9')>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection(
            "/login?pass=admin&name=admin' and (Select count(*) from data where uname='wucm' "
            "and mid(upass,1,1)>'a')>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

        res = libinjection.is_sql_injection(
            "/login?pass=admin&name=admin' and (Select count(*) from data where uname='wucm' "
            "and mid(upass,1,1)='w')>0 and 'a'='a")
        self.assertTrue(res['is_sqli'])

    # 反射型xss
    def test_xss1(self):
        res = libinjection.is_xss(
            "http://121.15.171.90:9006/?userName=<script>alert('反射型 XSS 攻击')</script>")
        self.assertTrue(res['is_xss'])

    # 存储型xss
    def test_xss2(self):
        res = libinjection.is_xss(
            "http://121.15.171.90:9006/?userName=<script>alert('存储型 XSS 攻击')</script>")
        self.assertTrue(res['is_xss'])


if __name__ == '__main__':
    unittest.main()
