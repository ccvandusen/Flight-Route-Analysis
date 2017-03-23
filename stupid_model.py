import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier


def get_accuracy_score(X_train, X_test, y_train, y_test, num_trees, max_features):
    '''
    INPUT: X_train,X_test,y_train,y_test
    OUTPUT: score, number of trees
    '''
    rf = RandomForestClassifier(
        n_estimators=num_trees, max_features=max_features)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    return rf.score(X_test, y_test), max_features


def get_and_group_data(filepath):
    data = pd.read_csv(filepath)
    groupby = data.groupby(['route']).mean()
    y = groupby['Closure']
    del groupby['Closure']
    X = groupby
    return X, y

if __name__ == '__main__':
    X, y = get_and_group_data('test.csv')
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.2)
    print get_accuracy_score(X_train, X_test, y_train, y_test, 1000, 6)
