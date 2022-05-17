# coding:utf-8
import math
# from enum import Enum
import threading
import re

import pandas as pd
import numpy as np
import libinjection



NORMAL = 0
ABNORMAL = 1


data_path = 'ueba.xlsx'
stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
             "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
             "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
             "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
             "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
             "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
             "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
             "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
             "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
             "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "user", "events", "repos",
             "commit",
             "api", "action", "explore", "issues", "search", "milestones", "settings", "notifications", "org", "create",
             "migrate"]


class WordCounter:
    _instance_lock = threading.Lock()
    __init_flag = False

    def __new__(cls, *args, **kwargs):
        if not hasattr(WordCounter, "_instance"):
            with WordCounter._instance_lock:
                if not hasattr(WordCounter, "_instance"):
                    WordCounter._instance = object.__new__(cls)
        return WordCounter._instance

    def __init__(self):
        if self.__init_flag:
            return

        self.__counts = {}
        self.__load_data()
        self.__word_count(self.__requests)
        self.__init_flag = True

    '''
    读取系统历史请求数据
    '''

    def __load_data(self):
        data = pd.read_excel(data_path)
        self.__requests = np.array(data['request'].str.replace('[0-9]*', ''))
        self.__doc_num = len(self.__requests)

    '''
    统计用户请求WordCount
    '''

    def __word_count(self, requests):
        for request in requests:
            words = request.split('/')
            for word in words:
                if word == '' or word in stopwords:
                    continue
                if word in self.__counts:
                    self.__counts[word] += 1
                else:
                    self.__counts[word] = 0

    def word_counts(self):
        return self.__counts

    def doc_nums(self):
        return self.__doc_num


class NaiveBayesInspector:
    def __init__(self, threshold):
        self.__threshold = threshold
        self.__counter = WordCounter()

    '''
    检测访问url是否符合用户历史行为
    '''

    def inspect(self, word_list):
        word_counts = self.__counter.word_counts()
        possibility = 0
        for word in word_list:
            if word in stopwords:
                continue
            if word not in word_counts:
                possibility += math.log(1.0 / (len(word_list) + 1) / (self.__counter.doc_nums() + 1))
            else:
                possibility += math.log(word_counts[word] / self.__counter.doc_nums())

        if possibility < self.__threshold:
            return ABNORMAL
        else:
            return NORMAL


'''
过滤无关请求
'''


def filter_request(request):
    # 资源文件访问过滤
    if request.find('.js') != -1 or request.find('.css') != -1:
        return True
    # favicon过滤
    if request.find('favicon') != -1:
        return True
    # 图像请求过滤
    if request.find('.png') != -1 or request.find('avatar') != -1:
        return True

    return False


'''
正则+截断
'''


def std_request(request):
    res = ''
    question_mark_pos = request.find('?')
    # 只取'?'之前的字符
    if question_mark_pos != -1:
        res = request[:question_mark_pos]

    # 去除标点符号
    r = "[A-Za-z0-9_.!+-=——,$%^，。？、~@#￥%……&*《》<>「」{}【】()]"
    res = re.sub(r, '', res)
    return res


def count_abnormal_operations(data, users):
    new_data = data['request'].str.replace('[0-9]*', '')
    data['request'] = new_data
    result = {}
    # inspector = NaiveBayesInspector(-20)
    for user in users:
        if user not in result:
            result[user] = 0
        test_data = data.loc[data['username'] == user, ['request']].values.tolist()
        # 检测逻辑 NaiveBayes -> libinjection
        for request in test_data:
            if filter_request(request[0]):
                continue
            sqli_res = libinjection.is_sql_injection(request[0])
            xss_res = libinjection.is_xss(request[0])
            if sqli_res['is_sqli'] or xss_res['is_xss']:
                result[user] += 1

    return result


def set_url_count(data, url_dict):
    data['url_count'] = data['request']
    for i in url_dict:
        data.loc[data['username'] == i, ['url_count']] = url_dict[i]
    return data
