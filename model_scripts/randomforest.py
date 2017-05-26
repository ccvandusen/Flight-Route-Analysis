import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.ensemble import RandomForestClassifier,
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import datetime
import model_data_prep as mdp
import feature_engineer as fe


def get_logit_results(X, y):
    model = sm.Logit(y, X).fit()  # using statsmodels because it doesn't apply
    # Regularization so we can get non-normalized
    # coefficient values
    return model.summary()


def plot_ROC_curve(X_train, y_train, X_test, labels):
    model = LogisticRegression()  # Using sklearn here because it has way more
    # built in functionality to get better predictions
    # compared to statsmodels
    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
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
    plt.title("ROC plot model")
    plt.show()
    return tprs, fprs, thresholds.tolist()


def random_forest_cross_val(X, y, num_trees=50, max_features=3, folds=10, n_jobs=-1):
    '''
    INPUT: X,y
    OUTPUT: printed scores, fitted model
    Prints out the accuracy, precision, and recall scores for a random forest on the data
    '''
    rf = RandomForestClassifier(
        n_estimators=num_trees, max_features=max_features, n_jobs=n_jobs)
    accuracy = cross_val_score(rf, X, y, cv=folds, n_jobs=n_jobs)
    precision = cross_val_score(
        rf, X, y, cv=folds, n_jobs=n_jobs, scoring='precision')
    recall = cross_val_score(
        rf, X, y, cv=folds, n_jobs=n_jobs, scoring='recall')
    print 'accuracy_scores : {}'.format(sum(accuracy) / len(accuracy))
    print 'precision_scores : {}'.format(sum(precision) / len(precision))
    print 'recall_scores : {}'.format(sum(recall) / len(recall))
    return rf.fit(X, y)


def gradient_boost_cross_val(X, y, num_trees=50, folds=10):
    '''
    INPUT: data, parameters for sklearn's gradient boost
    OUTPUT: cross validation scores for gradient boost
    '''
    gb = GradientBoosingClassifier(
        n_estimators=num_trees, max_depth=3, folds=10)
    accuracy = cross_val_score(rf, X, y, cv=folds, n_jobs=n_jobs)
    precision = cross_val_score(
        rf, X, y, cv=folds, scoring='precision')
    recall = cross_val_score(
        rf, X, y, cv=folds, scoring='recall')
    print 'accuracy_scores : {}'.format(sum(accuracy) / len(accuracy))
    print 'precision_scores : {}'.format(sum(precision) / len(precision))
    print 'recall_scores : {}'.format(sum(recall) / len(recall))
    return gb.fit(X, y)


def grid_search(model):
    '''
    INPUT: sklearn object: fitted model to grid Search
    OUTPUT: dict: contains the optimal parameters of the GridSearch
            float: accuracy score of the model w/ optimal parameters
    Implements a grid search
    '''
    # Parameters used to gridsearch, these are the optimized one for our final
    # Boosted model
    param_grid = {'n_estimators': [100, 200, 400], 'max_depth': [None, 1, 2, 4, 8], 'min_samples_leaf': [1, 9, 17],
                  'max_features': [1.0, 0.3, 'auto']}

    # Gridsearch object
    gsearch1 = GridSearchCV(model, param_grid)

    return gsearch1.best_params_, gsearch1.best_score_


# if __name__ == '__main__':
    #startTime = datetime.now()
    # print model_df.head(1), model_df.columns
    # y = model_df['4ClosureIndicator']
    # X = model_df.drop(['4ClosureIndicator', '4FlightTotal'], axis=1)
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.2)
    # plot_ROC_curve(X_train, y_train, X_test, y_test)
    # print datetime.now() - startTime
