import json
import pcaps2json as p2j
import filter
import pandas as pd
import multiprocessing as mp
import pickle

benign_url_file = pd.read_csv("majestic_million.csv")
benign_urls = {}

# Creates dictionary of benign urls from magestic millions csv
print ("start reading millions")
fileTail = len(benign_url_file.index)

def process_wrapper(chunkStart, chunkSize):
    limit = min(chunkStart + chunkSize, fileTail)
    for i in range(chunkStart, limit, 1):
        benign_urls[str(benign_url_file["Domain"][i])] = ""


def chunkify(size=5000):
    chunkTail = 0
    while True:
        chunkStart = chunkTail
        chunkTail += size
        yield chunkStart, chunkTail - chunkStart
        if chunkTail >= fileTail:
            break

cores = 5
pool = mp.Pool(cores)

for chunkStart, chunkSize in chunkify(5000):
    pool.apply_async(process_wrapper, (chunkStart, chunkSize, ))

pool.close()
pool.join()

filter.setBenignUrls(benign_urls)
print( "finish")

jsons = p2j.pcap2json()
print (len(jsons))
new_jsons = filter.getFilteredJsons("ben", jsons)
validation_dataset = pd.DataFrame(new_jsons)

with open('./etd_rf_clf.model', 'rb') as fr:
    tls_clf = pickle.load(fr)

validation_dataset = validation_dataset[validation_dataset.duplicated(keep=False) == False]
print (validation_dataset['000a'].value_counts())
print (tls_clf.predict(validation_dataset))



