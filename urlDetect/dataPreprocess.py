# represent the Urls by vectors

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm
import urllib
from string import digits
import numpy as np
import string
# from urlDetect.urlDetect import detectBatch, detect
import os
import pickle
good = './normal_request.txt'
bad = './badqueries.txt'

ngram = 2

def getdata():
    with open(good, 'r', encoding='utf-8') as f:
        good_query_list = [i.strip('\n') for i in f.readlines()[:]]

    with open(bad, 'r', encoding='utf-8') as f:
        bad_query_list = [urllib.parse.quote(i.strip('\n')) for i in f.readlines()[:]]
    return [good_query_list, bad_query_list]

def getTransformer():
    data = getdata()
    vectorizer = TfidfVectorizer(tokenizer=get_ngrams)
    vec = vectorizer.fit(data[0])
    return vec

def getTransformer2():
    data = getdata()
    vectorizer = TfidfVectorizer(tokenizer=get_ngrams)
    vec = vectorizer.fit(data[1])
    return vec

def train():
    data = getdata()
    print(data[1])
    vec = getTransformer()
    #temp = [str.translate(None, digits) for str in data[0]]
    # data[0] = temp
    X_train = vec.transform(data[0]) # normal request

    X_test = vec.transform(data[1]) # bad request
    # print(X_test.shape
    # print(X
    # X_train, X_test= train_test_split(X, test_size=0.2, random_state=42)
    clf = svm.OneClassSVM(nu=0.004, kernel="rbf", gamma=0.05)
    clf.fit(X_train)

    y_predict_test = clf.predict(X_test)
    y_predict = clf.predict(X_train)
    n_error_test = y_predict_test[y_predict_test == -1].size
    n_error = y_predict[y_predict == -1].size
    n_error_content = np.array(data[0])[y_predict == -1]
    print(n_error_test)
    print(n_error)
    print(n_error_content)

    print("exporting model...")
    pickle.dump(clf, open("urlDetect2.model", 'wb'), protocol=3)
    print("exporting over")




# load model and test once a line
def deTect(data):
    model = pickle.load(open('urlDetect3.model', 'rb'))
    vec = getTransformer()
    X = vec.transform(data)
    print(X.shape)
    result = model.predict(X)
    print(result)
    if result[0] == -1:
        return {
            "result" : "abnormal"
        }
    else:
        return {
            "result": "normal"
        }

# load model and test in bulk
def detectBAtch(data):
    model = pickle.load(open('./urlDetect/urlDetect.model', 'rb'))
    vec = getTransformer()
    X = vec.transform(data)
    result = model.predict(X)
    return {
        "normal count": str(result[result == 1].size),
        "abnormal count": str(result[result == -1].size)
    }


# tokenizer function, this will make 3 grams of each query
def get_ngrams(query):
    tempQuery = str(query)
    ngrams = []
    for i in range(0, len(tempQuery)-2):
        ngrams.append(tempQuery[i:i+2])
    return ngrams


# deTect(["/hr3/personnel/lzh/lzhdisciplineedit.aspx	recid=1''"])
train()
