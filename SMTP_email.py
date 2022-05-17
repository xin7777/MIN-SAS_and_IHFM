# !/usr/bin/python3
# coding: utf-8

import smtplib

import configparser
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr
from email.utils import formataddr
from time import *


def format_addr(s):
  name, addr = parseaddr(s)
  return formataddr((Header(name, "utf-8").encode(), addr))

class Email():

  def __init__(self):
    self.conf = configparser.ConfigParser()
    self.conf.read("config/config.cfg")
    self.from_email = self.conf.get("Email","username") # 邮箱地址
    self.from_email_pwd = self.conf.get("Email","password")# 邮箱密码
    self.to_email = self.conf.get("Email","receiver") # 接收者邮箱
    self.smtp_server = self.conf.get("Email","mail_host")# 协议

  def send(self, message):
    
    try:
        msg = MIMEText(str(message))
        msg["From"] = format_addr("%s" % (self.from_email))
        msg["To"] = format_addr("%s" % (self.to_email))
        msg["Subject"] = Header("安全感知系统告警", "utf-8").encode()

        server = smtplib.SMTP_SSL(self.smtp_server, port=int(self.conf.get("Email","port"))) # 腾讯企业邮箱配置（SSL）
        server.set_debuglevel(1)
        server.login(self.from_email, self.from_email_pwd)
        server.sendmail(self.from_email, [self.to_email], msg.as_string())
        print("发送成功")
        server.quit()
    except Exception as e:
        print(e)

    #事件发生后的阻塞时间
    '''
    try:
        msg_json = message
        if msg_json["type"] == "Scan":
            sleep(int(self.conf.get("Email","web_attack_sleep")))
        elif msg_json["type"] == "DDoS":
            sleep(int(self.conf.get("Email","web_attack_sleep"))*3)
    except Exception as e:
        print(e,type(message))
    '''
    

