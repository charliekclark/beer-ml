from enum import Enum
from typing import Self, Optional

from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd

class IngestAndPrepare(BaseEstimator, TransformerMixin):
    def __init__(self):
        """
        Description:
            Ensures all sort/drop transforms are consistent across both X and y.
            Use as first method in pipeline ALWAYS.
            
        """        
        pass
    
    def _lowercase_columns(self, X: pd.DataFrame) -> pd.DataFrame:
        X.columns = X.columns.str.lower()
        return X
    
    def _sort_columns(self, X: pd.DataFrame) -> pd.DataFrame:
        X.sort_index(axis=1, inplace=True)
        return X

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        self.X_ = X
        # lowercase all column names
        self.X_ = self._lowercase_columns(self.X_)
        # sort column names alphabetically
        self.X_ = self._sort_columns(self.X_)

        # Extract numeric and categorical columns
        self.numeric_columns_ = X.select_dtypes(include=['int64', 'float64']).columns
        self.categorical_columns_ = X.select_dtypes(include=['category', 'object']).columns

        # Store bounds/unique values to check for issues later
        self.numeric_bounds_ = {}
        for col in self.numeric_columns_:
            self.numeric_bounds_[col] = {
                'min': X[col].min(),
                'max': X[col].max(),
            }
        self.categorical_bounds_ = {}
        for col in self.categorical_columns_:
            # store values as list, preserving None/nan for accurate comparison later
            unique_vals = list(X[col].unique())
            self.categorical_bounds_[col] = unique_vals
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        # lowercase all column names
        X = self._lowercase_columns(X)
        # sort column names alphabetically
        X = self._sort_columns(X)
        
        # Check consistency of X
        if set(self.X_.columns) != set(X.columns):
            unexpected_columns = set(self.X_.columns) - set(X.columns)
        if set(X.columns) - set(self.X_.columns):
            missing_columns = set(X.columns) - set(self.X_.columns)
            raise ValueError(f"IngestAndPrepare: was fit with {self.X_.columns} columns, but was given {X.columns} columns to transform.\nMissing columns: {missing_columns}\nUnexpected columns: {unexpected_columns}")

        # Print warnings if numeric columns are not within bounds
        for col in self.numeric_columns_:
            if X[col].min() < self.numeric_bounds_[col]['min']:
                print(f"WARNING (IngestAndPrepare): {col} has values less than the minimum training value: {self.numeric_bounds_[col]['min']}. You will be extrapolating outside of the training data range.")
            if X[col].max() > self.numeric_bounds_[col]['max']:
                print(f"WARNING (IngestAndPrepare): {col} has values greater than the maximum training value: {self.numeric_bounds_[col]['max']}. You will be extrapolating outside of the training data range.")

        # Print warnings if categorical columns are not within bounds
        for col in self.categorical_columns_:
            unidentified = []
            for val in X[col].unique():
                if pd.isnull(val) == False and val not in self.categorical_bounds_[col]:
                    unidentified.append(val)
            if len(unidentified) > 0:
                print(f"WARNING (IngestAndPrepare): {col} has values not seen within the training data: {unidentified}.  You will be extrapolating outside of the training data range.")
        return X

class CastToCategory(BaseEstimator, TransformerMixin):
    def __init__(self, variable):
        """
        Arguments:
            1) variable (string): name of the variable to cast to category
            
        Description:
            Converts variable to category
        
        """
        self.variable = variable

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"CastToCategory: variable {self.variable} not found in X")
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        vartype = str(X[self.variable].dtype)
        if 'int' in vartype or 'float' in vartype:
            X[self.variable] = X[self.variable].apply(lambda x: None if pd.isnull(x) else str(int(x)))
        else:
            X[self.variable] = X[self.variable].apply(lambda x: None if pd.isnull(x) else str(x))
        return X

class CommonCaseMethod(Enum):
    LOWER = 'lower'
    UPPER = 'upper'
    TITLE = 'title'

class CommonCaseClasses(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, method: str = CommonCaseMethod.LOWER.value):
        """
        Arguments:
            1) variable (string): name of the categorical variable to common case
            2) method (set string): style of casing
                a. options: 'lower', 'upper', 'title'
            
        Description:
            Converts all values to desired casing and strips residual spaces
        
        """
        # Check if method is valid
        valid_methods = [e.value for e in CommonCaseMethod]
        if method not in valid_methods:
            raise ValueError(f"CommonCaseClasses: method must be one of {valid_methods}")
        self.variable = variable
        self.method = method

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"CommonCaseClasses: variable {self.variable} not found in X")        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method == CommonCaseMethod.LOWER.value:
            X[self.variable] = X[self.variable].apply(lambda x: str(x).lower().strip() if pd.isnull(x) == False else None)
        elif self.method == CommonCaseMethod.UPPER.value:
            X[self.variable] = X[self.variable].apply(lambda x: str(x).upper().strip() if pd.isnull(x) == False else None)
        elif self.method == CommonCaseMethod.TITLE.value:
            X[self.variable] = X[self.variable].apply(lambda x: str(x).title().strip() if pd.isnull(x) == False else None)
        return X

class ImputeMissingClassesMethod(Enum):
    MODE = 'mode'
    CAST = 'cast'
    DROP = 'drop'

class ImputeMissingClasses(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, method: str = ImputeMissingClassesMethod.MODE.value, cast_text: str = '#Missing'):
        """
        Arguments:
            1) variable (string): name of the categorical variable to impute missing values
            2) method (set string): method to impute missing classes
                a. options: 'mode', 'cast', 'drop'
                    i. if using 'cast', all missing values will be replaced with cast_text
            3) cast_text (string): text to replace missing values with if method == 'cast'
            
        Description:
            Imputes/drops missing values
        
        """
        self.variable = variable
        self.cast_text = cast_text
        # Check if method is valid
        valid_methods = [e.value for e in ImputeMissingClassesMethod]
        if method not in valid_methods:
            raise ValueError(f"ImputeMissingClasses: method must be one of {valid_methods}")
        
        if method == ImputeMissingClassesMethod.DROP.value:
            print(f"WARNING (ImputeMissingClasses): you are dropping missing values. This may result in a loss of information during inference!")
        self.method = method

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"ImputeMissingClasses: variable {self.variable} not found in X")   

        if self.method == ImputeMissingClassesMethod.MODE.value:
            self.cast_text= X[self.variable].mode().values[0]
        return self        
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method != ImputeMissingClassesMethod.DROP.value:
            X[self.variable].fillna(self.cast_text, inplace=True)
        else:
            X.dropna(subset = [self.variable], inplace=True)
        return X

class HandleRareClassesMethod(Enum):
    CAST = 'cast'
    DROP = 'drop'

class HandleRareClasses(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, method: str = HandleRareClassesMethod.CAST.value, cast_text: str = '#Other', threshold: float = .02):
        """
        Arguments:
            1) variable (string): name of the categorical variable to handle rare classes for
            2) method (set string): method to handle rare classes
                a. options: 'cast', 'drop'
                    i. if using 'cast', all missing values will be replaced with cast_text
            3) cast_text (string): text to replace missing values with if method == 'cast'
            4) threshold (float): the percent at and below which (<=) a class will be considered as rare
            
        Description:
            Handles rare classes
        
        """
        self.variable = variable
        self.cast_text = cast_text
        if threshold < 0 or threshold > 1:
            raise ValueError(f"HandleRareClasses: threshold must be between 0 and 1 (recommended: .01-.05). Received {threshold}.")
        self.threshold = threshold

        # Check if method is valid
        valid_methods = [e.value for e in HandleRareClassesMethod]
        if method not in valid_methods:
            raise ValueError(f"HandleRareClasses: method must be one of {valid_methods}")
        
        if method == HandleRareClassesMethod.DROP.value:
            print(f"WARNING (HandleRareClasses): you are dropping rare classes. This may result in a loss of information during inference!")
        self.method = method

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        self.rare_classes_ = []
        if self.method == HandleRareClassesMethod.CAST.value:
            temp = X[self.variable].value_counts(normalize=True)
            self.rare_classes_ = list(temp[temp <= self.threshold].index)
        return self        
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method == HandleRareClassesMethod.DROP.value:
            X = X[~X[self.variable].isin(self.rare_classes_)]
        else:
            X.loc[X[self.variable].isin(self.rare_classes_), self.variable] = self.cast_text
        return X

class ImputeMissingNumbersMethod(Enum):
    MEAN = 'mean'
    MEDIAN = 'median'
    DROP = 'drop'

class ImputeMissingNumbers(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, method: str = ImputeMissingNumbersMethod.MEAN.value):
        """
        Arguments:
            1) variable (string): name of the variable to impute
            2) method (set string): missing value treatment method
                a. options: 'mean', 'median', 'drop'
        
        Description:
            Imputes/drops missing values
        
        """
        self.variable = variable
        # Check if method is valid
        valid_methods = [e.value for e in ImputeMissingNumbersMethod]
        if method not in valid_methods:
            raise ValueError(f"ImputeMissingNumbers: method must be one of {valid_methods}")
        if method == ImputeMissingNumbersMethod.DROP.value:
            print(f"WARNING (ImputeMissingNumbers): you are dropping missing values. This may result in a loss of information during inference!")

        self.method = method

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"ImputeMissingNumbers: variable {self.variable} not found in X")   

        if self.method == ImputeMissingNumbersMethod.MEAN.value:
            self.impute_val_= X[self.variable].mean()
        elif self.method == ImputeMissingNumbersMethod.MEDIAN.value:
            self.impute_val_= X[self.variable].median()
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method != ImputeMissingNumbersMethod.DROP.value:
            X[self.variable].fillna(self.impute_val_, inplace=True)
        else:
            X.dropna(subset = [self.variable], inplace=True)
        return X

class OutliersMethod(Enum):
    WINSORIZE = 'winsorize'
    DROP = 'drop'

class ZOutliers(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, sigma: float = 3, method: str = OutliersMethod.WINSORIZE.value):
        """
        Arguments:
            1) variable (string): name of the variable to impute
            2) sigma (float): how many standard deviations away from the mean you wish to winsorize for
            3) method (set string): method to treat outliers
                a. options: 'winsorize', 'drop'

        Description:
            Winsorizes outliers at the desired sigma thresholds
        
        """
        self.variable = variable
        self.sigma = sigma
        # Check if method is valid
        valid_methods = [e.value for e in OutliersMethod]
        if method not in valid_methods:
            raise ValueError(f"ZOutliers: method must be one of {valid_methods}")
        if method == OutliersMethod.DROP.value:
            print(f"WARNING (ZOutliers): you are dropping outliers. This may result in a loss of information during inference!")
        self.method = method
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"ZOutliers: variable {self.variable} not found in X")   

        sigma = X[self.variable].std()
        mu = X[self.variable].mean()
        
        self.l_thresh_ = mu - self.sigma*sigma
        self.u_thresh_ = mu + self.sigma*sigma
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method == OutliersMethod.WINSORIZE.value:
            X[self.variable] = X[self.variable].apply(lambda x: self.l_thresh_ if x < self.l_thresh_ else x)
            X[self.variable] = X[self.variable].apply(lambda x: self.u_thresh_ if x > self.u_thresh_ else x)
        else:
            X = X[X[self.variable] > self.l_thresh_]
            X = X[X[self.variable] < self.u_thresh_]
        return X

class ManualOutliersDirection(Enum):
    LEFT = 'left'
    RIGHT = 'right'

class ManualOutliers(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, value: float, direction: str, method: str = OutliersMethod.WINSORIZE.value):
        """
        Arguments:
            1) variable (string): name of the variable to impute
            2) value (float): the value at which (inclusive) you wish to consider an outlier
            3) direction (set string): the tail direction of the outlier
                a. options: 'left', 'right'
            3) method (set string): method to treat outliers
                a. options: 'winsorize', 'drop'

        Description:
            Winsorizes outliers at the desired threshold
        
        """
        self.variable = variable
        self.value = value
        # Check if direction is valid
        valid_directions = [e.value for e in ManualOutliersDirection]
        if direction not in valid_directions:
            raise ValueError(f"ManualOutliers: direction must be one of {valid_directions}")
        self.direction = direction

        # Check if method is valid
        valid_methods = [e.value for e in OutliersMethod]
        if method not in valid_methods:
            raise ValueError(f"ManualOutliers: method must be one of {valid_methods}")
        if method == OutliersMethod.DROP.value:
            print(f"WARNING (ManualOutliers): you are dropping outliers. This may result in a loss of information during inference!")
        self.method = method
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"ManualOutliers: variable {self.variable} not found in X")   
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        if self.method == OutliersMethod.WINSORIZE.value:
            if self.direction == ManualOutliersDirection.LEFT.value:
                X[self.variable] = X[self.variable].apply(lambda x: self.value if x < self.value else x)
            elif self.direction == ManualOutliersDirection.RIGHT.value:
                X[self.variable] = X[self.variable].apply(lambda x: self.value if x > self.value else x)
        else:
            if self.direction == ManualOutliersDirection.LEFT.value:
                X = X[X[self.variable] > self.value]
            elif self.direction == ManualOutliersDirection.RIGHT.value:
                X = X[X[self.variable] < self.value]
        return X