from typing import Optional

import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from sklearn.metrics import max_error as me, mean_absolute_error as mae, r2_score, mean_squared_error as mse,mean_absolute_percentage_error as mape
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from sklearn.model_selection import train_test_split


def evaluate_regression(tr_act: pd.Series, tr_pred: pd.Series, te_act: Optional[pd.Series] = None, te_pred: Optional[pd.Series] = None) -> pd.DataFrame:
    """
    Arguments:
        1) tr_act (pandas series/numpy array): training actual target values
        2) tr_pred (pandas series/numpy array): training predicted target values
        3) te_act (pandas series/numpy array): testing actual target values
        4) te_pred (pandas series/numpy array): testing predicted target values

    Description:
        Generates a statistical summary of predictions vs actuals for a regression problem
        
    Returns:
        1) pandas dataframe
    """
    pd.options.display.float_format = '{:.4f}'.format

    metrics = ['Correl','R^2', 'MAE', 'RMSE', 'MAPE', 'Max Err']

    def compute_stats(y_true, y_pred, allow_corr=True):
        stats_fns = [
            (lambda a, b: pearsonr(a, b)[0] if allow_corr else None),
            lambda a, b: r2_score(a, b),
            lambda a, b: mae(a, b),
            lambda a, b: mse(a, b)**0.5,
            lambda a, b: mape(a, b),
            lambda a, b: me(a, b)
        ]
        return [f(y_true, y_pred) for f in stats_fns]

    ave = np.ones(len(tr_act)) * tr_act.mean()
    blind_stats = compute_stats(tr_act, ave, allow_corr=False)
    data = pd.DataFrame({'metric': metrics, 'base': blind_stats})

    tr_stats = compute_stats(tr_act, tr_pred)
    data['train'] = tr_stats

    if te_act is not None:
        te_stats = compute_stats(te_act, te_pred)
        data['test'] = te_stats

    return data

class ConfusionMatrix:
    def __init__(self):
        pass
    
    def fit(self, actual: pd.Series, predicted: pd.Series):
        # Convert to Series and reset index to ensure alignment by position
        # This ensures that row i in actual corresponds to row i in predicted
        actual = pd.Series(actual.values, name="truth")
        if isinstance(predicted, pd.Series):
            predicted = pd.Series(predicted.values, name="pred")
        else:
            predicted = pd.Series(predicted, name="pred")
        
        # Verify lengths match
        if len(actual) != len(predicted):
            raise ValueError(f"ConfusionMatrix: actual and predicted must have the same length. Got {len(actual)} and {len(predicted)}")
        
        predicted = pd.Series(predicted.values, name="pred")
        
        # Verify lengths match
        if len(actual) != len(predicted):
            raise ValueError(f"ConfusionMatrix: actual and predicted must have the same length. Got {len(actual)} and {len(predicted)}")
        
        # Check for NaN values and warn if found
        nan_mask = actual.isna() | predicted.isna()
        if nan_mask.any():
            nan_count = nan_mask.sum()
            print(f"WARNING (ConfusionMatrix): {nan_count} rows with NaN values were excluded from the confusion matrix")
        
        # Drop NaN values explicitly to ensure all valid rows are counted
        mask = ~nan_mask
        actual_clean = actual[mask]
        predicted_clean = predicted[mask]
        
        if len(actual_clean) == 0:
            raise ValueError("ConfusionMatrix: No valid (non-NaN) rows found in actual and predicted")
        
        self.confusion_matrix = pd.crosstab(actual_clean, predicted_clean)
        self.confusion_matrix_rel = pd.crosstab(actual_clean, predicted_clean, normalize='all')


def evaluate_classification(
    tr_act: pd.Series, tr_pred: pd.Series, te_act: Optional[pd.Series] = None, te_pred: Optional[pd.Series] = None
) -> tuple:
    """
    Arguments:
        1) tr_act (pandas series): training actual target values
        2) tr_pred (pandas series): training predicted target values
        3) te_act (pandas series): testing actual target values
        4) te_pred (pandas series): testing predicted target values

    Description:
        Generates a statistical summary of predictions vs actuals for a classification problem
        Designed only to work for binary classification

    Returns:
        1) pandas dataframe of metrics
        2) base model confusion matrix
        3) training confusion matrix
        4) testing confusion matrix (if test supplied)
    """
    pd.options.display.float_format = '{:.4f}'.format
    metrics = ['Accuracy', 'Precision', 'Recall', 'F1']

    # Use sorted unique for binary class, highest label as pos
    pos = np.sort(tr_act.unique())[1]  # assumes binary classification

    def get_stats(true, pred):
        return [
            accuracy_score(true, pred),
            precision_score(true, pred, average='binary', pos_label=pos),
            recall_score(true, pred, average='binary', pos_label=pos),
            f1_score(true, pred, average='binary', pos_label=pos)
        ]

    def get_cm(true, pred):
        CM = ConfusionMatrix()
        CM.fit(true, pred)
        return CM

    # Blind/base model: always predict majority class
    blind_pred = np.full(len(tr_act), tr_act.mode().values[0])
    stats_base = get_stats(tr_act, blind_pred)
    blindcm = get_cm(tr_act, blind_pred)

    # Training prediction stats and confusion
    stats_train = get_stats(tr_act, tr_pred)
    traincm = get_cm(tr_act, tr_pred)
    data = pd.DataFrame({'metric': metrics, 'base': stats_base, 'train': stats_train})

    # Optionally handle test statistics/confusion matrix if supplied
    if te_act is not None and te_pred is not None:
        stats_test = get_stats(te_act, te_pred)
        testcm = get_cm(te_act, te_pred)
        data['test'] = stats_test
        return data, blindcm, traincm, testcm

    return data, blindcm, traincm, None