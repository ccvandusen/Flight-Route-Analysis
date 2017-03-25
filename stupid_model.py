import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import datetime
import multiprocessing


def fit_log_reg_model(X_train, X_test, y_train):
    model = LogisticRegression()
    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    return model, probabilities


def plot_ROC_curve(model, probabilities, labels):
    thresholds = np.sort(probabilities)
    tprs = []
    fprs = []

    num_positive_cases = sum(labels)
    num_negative_cases = len(labels) - num_positive_cases

    for threshold in thresholds:
        # With this threshold, give the prediction of each instance
        predicted_positive = probabilities >= threshold
        # Calculate the number of correctly predicted positive cases
        true_positives = np.sum(predicted_positive * labels)
        # Calculate the number of incorrectly predicted positive cases
        false_positives = np.sum(predicted_positive) - true_positives
        # Calculate the True Positive Rate
        tpr = true_positives / float(num_positive_cases)
        # Calculate the False Positive Rate
        fpr = false_positives / float(num_negative_cases)

        fprs.append(fpr)
        tprs.append(tpr)
    x = np.arange(0, 1.1, 0.1)
    y = x
    plt.plot(fprs, tprs)
    plt.plot(x, y)
    plt.plot(fprs, tprs)
    plt.xlabel("False Positive Rate (1 - Specificity)")
    plt.ylabel("True Positive Rate (Sensitivity, Recall)")
    plt.title("ROC plot of fake data")
    plt.show()
    return tprs, fprs, thresholds.tolist()


def run_random_forest(X_train, X_test, y_train, y_test, num_trees, max_features):
    '''
    INPUT: X_train,X_test,y_train,y_test
    OUTPUT: score, number of trees
    '''
    rf = RandomForestClassifier(
        n_estimators=num_trees, max_features=max_features, n_jobs=-1)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    return rf.score(X_test, y_test), max_features


def get_and_group_data(filepath):
    data = pd.read_csv(filepath, nrows=100000)
    groupby = data.groupby(['route']).mean()
    groupby = groupby[np.isfinite(groupby['Closure'])]
    groupby.dropna(inplace=True)
    y = groupby['Closure']
    del groupby['Closure']
    X = groupby[['ArrDelay', 'DepDelay', 'Distance',
                 'NASDelay']]
    return groupby, X, y

if __name__ == '__main__':
    startTime = datetime.now()
    groupby, X, y = get_and_group_data('data/2005_indicators.csv')
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.2)
    model, probabilities = fit_log_reg_model(X_train, X_test, y_train)
    tprs, fprs, thresholds = plot_ROC_curve(model, probabilities, y_test)
    print datetime.now() - startTime
