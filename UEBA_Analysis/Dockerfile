# 基础镜像
FROM python:3.6.12-buster
# 维护人
MAINTAINER wzq<wzhq199@163.com>
# 拷贝python第三方库
COPY ./dependencies/ /usr/local/dependencies/
# 拷贝UEBA源代码
COPY ./ /usr/local/UEBA_analysis/
# 安装python第三方库
RUN pip install /usr/local/dependencies/*
RUN python /usr/local/UEBA_analysis/ueba_analyze.py
CMD echo "--------finish--------"
CMD /bin/bash