from enum import Enum
from typing import Self, Optional, List

from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd

class OneHotEncodeMethod(Enum):
    FREQ = 'freq'
    FIRST = 'first'
    LAST = 'last'

class OneHotEncode(BaseEstimator, TransformerMixin):
    def __init__(self, columns: List[str], method: str = OneHotEncodeMethod.FREQ.value):
        """
        Arguments:
            1) columns (list of strings): list of all categorical variables to encode
            2) method (set string): method to determine which class to drop
                a. options: 'freq', 'first', 'last'
                    i. 'freq': drops the most frequent class
                    ii. 'first': drops the first class (alphabetically sorted)
                    iii. 'last': drops the last class (alphabetically sorted)

        Description:
            One-hot encodes the independent variables of the input dataframe
        
        """
        self.columns = columns.copy() if columns else []
        
        # Check if method is valid
        valid_methods = [e.value for e in OneHotEncodeMethod]
        if method not in valid_methods:
            raise ValueError(f"OneHotEncode: method must be one of {valid_methods}")
        self.method = method
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if columns list is empty
        if not self.columns:
            raise ValueError("OneHotEncode: columns list cannot be empty")
        
        # Check if all columns exist in X
        missing_columns = [col for col in self.columns if col not in X.columns]
        if missing_columns:
            raise ValueError(f"OneHotEncode: the following columns were not found in X: {missing_columns}")
        
        # Build the hot_map_ dictionary and track dropped categories
        self.hot_map_ = {}
        self.dropped_categories_ = {}
        self.all_fit_categories_ = {}
        
        for c in self.columns:
            if self.method == OneHotEncodeMethod.FREQ.value:
                freq = X[c].value_counts()
                if len(freq) == 0:
                    raise ValueError(f"OneHotEncode: column '{c}' has no values to encode")
                all_categories = list(freq.index)
                self.hot_map_[c] = all_categories[:-1]
                self.dropped_categories_[c] = all_categories[-1]
            elif self.method == OneHotEncodeMethod.FIRST.value:
                unq = X[c].unique()
                if len(unq) == 0:
                    raise ValueError(f"OneHotEncode: column '{c}' has no values to encode")
                unq.sort()
                all_categories = list(unq)
                self.hot_map_[c] = all_categories[1:]
                self.dropped_categories_[c] = all_categories[0]
            elif self.method == OneHotEncodeMethod.LAST.value:
                unq = X[c].unique()
                if len(unq) == 0:
                    raise ValueError(f"OneHotEncode: column '{c}' has no values to encode")
                unq.sort()
                all_categories = list(unq)
                self.hot_map_[c] = all_categories[:-1]
                self.dropped_categories_[c] = all_categories[-1]
            
            # Store all categories seen during fit (including dropped one)
            self.all_fit_categories_[c] = set(self.hot_map_[c] + [self.dropped_categories_[c]])
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if hot_map_ exists (i.e., fit was called)
        if not hasattr(self, 'hot_map_'):
            raise ValueError("OneHotEncode: must call fit before transform")
        
        # Check if all columns from fit exist in transform X
        missing_columns = [col for col in self.hot_map_.keys() if col not in X.columns]
        if missing_columns:
            raise ValueError(f"OneHotEncode: the following columns were not found in X during transform: {missing_columns}")
        
        # Check for new categories in transform data
        for k in self.hot_map_.keys():
            transform_categories = set(X[k].unique())
            new_categories = transform_categories - self.all_fit_categories_[k]
            if new_categories:
                print(f"WARNING (OneHotEncode): column '{k}' has new categories in transform data that were not seen during fit: {new_categories}. These will be encoded as all zeros (same as the dropped category '{self.dropped_categories_[k]}').")
        
        # Perform one-hot encoding
        for k in self.hot_map_.keys():
            for c in self.hot_map_[k]:
                new_col_name = k + '_' + str(c).replace(' ', '_')
                X[new_col_name] = X[k].apply(lambda x: 1 if str(x) == str(c) else 0)
            X.drop(columns=[k], inplace=True)
            
        return X