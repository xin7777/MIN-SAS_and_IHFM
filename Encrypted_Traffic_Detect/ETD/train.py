import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import numpy as np
from sklearn.metrics import accuracy_score, confusion_matrix
import pickle
from sklearn.model_selection import train_test_split, KFold

#Import Datasets
malicious_dataset = pd.read_csv("./malicious_flows.csv")
benign_dataset = pd.read_csv("./sample_benign_flows.csv")

#
benign_dataset = benign_dataset[benign_dataset.duplicated(keep=False) == False]
all_flows = pd.concat([malicious_dataset, benign_dataset])

#Inspecting datasets for columns and rows with missing values
missing_values = all_flows.isnull().sum()
overall_percentage = (missing_values/all_flows.isnull().count())

# Isolating independent and dependent variables for training dataset
reduced_dataset = all_flows.sample(30000)
reduced_y = reduced_dataset['isMalware']
reduced_x = reduced_dataset.drop(['isMalware'], axis=1)
reduced_benign = benign_dataset.drop(['isMalware'], axis=1)
reduced_malicious = malicious_dataset.drop(['isMalware'], axis=1)


# Splitting datasets into training and test data
x_train, x_test, y_train, y_test = train_test_split(reduced_x, reduced_y, test_size=0.2, random_state=42)

# Training random forest classifier
rf_clf = RandomForestClassifier(max_depth=100)
rf_clf.fit(x_train, y_train)
rf_prediction = rf_clf.predict(x_test)
conf_m = confusion_matrix(y_test, rf_prediction)
print(conf_m)
print('Random Forest Classifier Accuracy score: ', accuracy_score(y_test, rf_prediction))

print "storing the model..."
filename = "etd_rf_clf.model"
pickle.dump(rf_clf, open(filename, "wb"))
