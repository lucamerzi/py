#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 03:51:56 2017

@author: antoinekrainc
"""

# Logistic Regression

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Importing the dataset
dataset = pd.read_csv('train.csv')
X = dataset.iloc[:,[2,4,5,6]].values
y = dataset.iloc[:, 1].values
                      
# Replacing NaN in Ages
from sklearn.preprocessing import Imputer
imputer = Imputer(missing_values="NaN", strategy="median", axis=0, verbose=0, copy=True)
imputer.fit(X[:, 2:3])
X[:, 2:3] = imputer.transform(X[:, 2:3])                      


# Encoding categorical data
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder = LabelEncoder()
X[:, 1] = labelencoder.fit_transform(X[:, 1])

onehotencoder = OneHotEncoder(categorical_features = [1])
X = onehotencoder.fit_transform(X).toarray()

X = X[:, 1:]

from scipy import stats
stats.chisqprob = lambda chisq, df: stats.chi2.sf(chisq, df)

import statsmodels.api as sm
X = np.append(arr = np.ones((891, 1)).astype(int), values = X, axis = 1)
logit_model = sm.Logit(y, X)
result = logit_model.fit()
result.summary()


# Splitting the dataset into the Training set and Test set
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 0)


# Fitting Logistic Regression to the Training set
from sklearn.linear_model import LogisticRegression
classifier = LogisticRegression(random_state = 0)
classifier.fit(X_train, y_train)

# Predicting the Test set results
y_pred = classifier.predict(X_test)

# Making the Confusion Matrix
from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)

