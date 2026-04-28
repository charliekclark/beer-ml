from enum import Enum
from typing import Self, Optional

from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class Ln(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, drop_orig: bool = False):
        """
        Arguments:
            1) variable (string): name of the variable to take log form
            2) drop_orig (boolean): if True, will drop the original variable

        Description:
            Takes the natural log of the desired variable
        
        """
        self.variable = variable
        self.drop_orig = drop_orig
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"Ln: variable {self.variable} not found in X")
        
        self.x_min_ = X[self.variable].min()
        # Push to positive realm
        if self.x_min_ <= 0:
            print(f"WARNING (Ln): {self.variable} contains values <= 0; implementing adjustment to make all values >= 1")

        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if fit was called
        if not hasattr(self, 'x_min_'):
            raise ValueError("Ln: must call fit before transform")
        
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"Ln: variable {self.variable} not found in X during transform")
        
        # Push to positive realm
        if X[self.variable].min() < self.x_min_:
            raise ValueError(f"Ln: cannot take log form of {self.variable} because min is <= 0 ({X.min()}) and the fit threshold of {self.x_min_}")
        else:
            X[self.variable] = X[self.variable] + abs(self.x_min_) + 1
        
        X['ln(' + self.variable + ')'] = np.log(X[self.variable])
        
        # Drop orig
        if self.drop_orig:
            X.drop(columns=[self.variable], inplace=True)
        return X

class Power(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, power: float, drop_orig: bool = False):
        """
        Arguments:
            1) variable (string): name of the variable to take power form
            2) power (float): power to take variable
                a. .5, 2, 3
            3) drop_orig (boolean): if True, will drop the original variable

        Description:
            Takes the power of the desired variable
        
        """
        self.variable = variable
        self.power = power
        self.drop_orig = drop_orig
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"Power: variable {self.variable} not found in X")
        
        self.x_min_ = X[self.variable].min()
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if fit was called
        if not hasattr(self, 'x_min_'):
            raise ValueError("Power: must call fit before transform")
        
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"Power: variable {self.variable} not found in X during transform")
        
        # Safety check
        transform_min = X[self.variable].min()
        if transform_min < self.x_min_ and transform_min < 0 and self.power < 0.5:
            raise ValueError(f"Power: cannot take power of {self.variable} because min is < 0 ({transform_min}), power = {self.power}, and fit transformation threshold ({self.x_min_}) < min")
        
        # Push to positive realm
        if self.power == 0.5 and self.x_min_ < 0:
            print(f"WARNING (Power): {self.variable} contains values < 0; implementing adjustment to make all values >= 0")
            X[self.variable] = X[self.variable] + abs(self.x_min_)
        
        X[self.variable + '^' + str(self.power)] = np.power(X[self.variable], self.power)
        
        # Drop orig
        if self.drop_orig:
            X.drop(columns=[self.variable], inplace=True)
        return X

class InteractOperator(Enum):
    ADD = '+'
    SUBTRACT = '-'
    MULTIPLY = '*'
    DIVIDE = '/'

class InteractDivZeroMethod(Enum):
    MEAN = 'mean'
    NULL = 'null'

class Interact(BaseEstimator, TransformerMixin):
    def __init__(self, variable1: str, variable2: str, operator: str, div_zero: str = InteractDivZeroMethod.MEAN.value):
        """
        Arguments:
            1) variable1 (string): name of the first variable to interact
            2) variable2 (string): name of the second variable to interact
            3) operator (set string): mathematical operator
                a. '+', '-', '*', '/'
            4) div_zero (set string): method to handle division by zero
                a. 'mean', 'null'

        Description:
            Interacts two variables
        
        """
        self.variable1 = variable1
        self.variable2 = variable2
        
        # Check if operator is valid
        valid_operators = [e.value for e in InteractOperator]
        if operator not in valid_operators:
            raise ValueError(f"Interact: operator must be one of {valid_operators}")
        self.operator_ = operator
        
        # Check if div_zero is valid
        valid_div_zero = [e.value for e in InteractDivZeroMethod]
        if div_zero not in valid_div_zero:
            raise ValueError(f"Interact: div_zero must be one of {valid_div_zero}")
        self.div_zero_ = div_zero
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variables are in X
        if self.variable1 not in X.columns:
            raise ValueError(f"Interact: variable {self.variable1} not found in X")
        if self.variable2 not in X.columns:
            raise ValueError(f"Interact: variable {self.variable2} not found in X")
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if variables are in X
        if self.variable1 not in X.columns:
            raise ValueError(f"Interact: variable {self.variable1} not found in X during transform")
        if self.variable2 not in X.columns:
            raise ValueError(f"Interact: variable {self.variable2} not found in X during transform")
        
        new_col_name = self.variable1 + self.operator_ + self.variable2
        
        # Perform operation
        if self.operator_ == InteractOperator.ADD.value:
            X[new_col_name] = X[self.variable1] + X[self.variable2]
        elif self.operator_ == InteractOperator.SUBTRACT.value:
            X[new_col_name] = X[self.variable1] - X[self.variable2]
        elif self.operator_ == InteractOperator.MULTIPLY.value:
            X[new_col_name] = X[self.variable1] * X[self.variable2]
        elif self.operator_ == InteractOperator.DIVIDE.value:
            X[new_col_name] = X[self.variable1] / X[self.variable2]
            
            # Handle division by zero
            zero_mask = (X[self.variable2] == 0)
            if zero_mask.any():
                print(f"WARNING (Interact): {new_col_name} encountered division by zero")
                X.loc[zero_mask, new_col_name] = np.inf
                X[new_col_name] = X[new_col_name].replace([np.inf, -np.inf], np.nan)
                
                if self.div_zero_ == InteractDivZeroMethod.MEAN.value:
                    mu = X[new_col_name].dropna().mean()
                    if pd.isna(mu):
                        raise ValueError(f"Interact: cannot compute mean for {new_col_name} because all values are NaN after division by zero")
                    X[new_col_name].fillna(mu, inplace=True)
                # else: leave as NaN (null method)
                
        
        return X

class ScalerMethod(Enum):
    Z = 'z'
    MINMAX = 'minmax'

class Scaler(BaseEstimator, TransformerMixin):
    def __init__(self, method: str = ScalerMethod.Z.value):
        """
        Arguments:
            1) method (set string): method of standardization
                a. options: 'z', 'minmax'

        Description:
            Standardizes the entire data set
        
        """
        # Check if method is valid
        valid_methods = [e.value for e in ScalerMethod]
        if method not in valid_methods:
            raise ValueError(f"Scaler: method must be one of {valid_methods}")
        self.method = method
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        self.params_ = {}
        cols = []
        
        # Get numeric columns
        for i, d in enumerate(X.dtypes):
            dtype_str = str(d)
            if 'int' in dtype_str or 'float' in dtype_str:
                cols.append(X.columns[i])
        
        if len(cols) == 0:
            raise ValueError("Scaler: no numeric columns found in X")
        
        # Compute parameters for each column
        if self.method == ScalerMethod.Z.value:
            for c in cols:
                mean_val = X[c].mean()
                std_val = X[c].std()
                if std_val == 0:
                    raise ValueError(f"Scaler: column {c} has zero standard deviation, cannot standardize")
                self.params_[c] = (mean_val, std_val)
        else:  # minmax
            for c in cols:
                min_val = X[c].min()
                max_val = X[c].max()
                if max_val == min_val:
                    raise ValueError(f"Scaler: column {c} has min == max, cannot scale")
                self.params_[c] = (min_val, max_val)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if fit was called
        if not hasattr(self, 'params_'):
            raise ValueError("Scaler: must call fit before transform")
        
        # Check if all columns from fit exist in transform X
        missing_columns = [col for col in self.params_.keys() if col not in X.columns]
        if missing_columns:
            raise ValueError(f"Scaler: the following columns were not found in X during transform: {missing_columns}")
        
        for k in self.params_.keys():
            p0 = self.params_[k][0]
            p1 = self.params_[k][1]
            if self.method == ScalerMethod.Z.value:
                X[k] = (X[k] - p0) / p1
            else:  # minmax
                X[k] = (X[k] - p0) / (p1 - p0)
        
        return X    

class CompressClassesMethod(Enum):
    LOWER = 'lower'
    UPPER = 'upper'
    TITLE = 'title'

class CompressClasses(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str, map_dict: dict, method: str = CompressClassesMethod.LOWER.value, default_group: Optional[str] = None):
        """
        Arguments:
            1) variable (string): name of the categorical variable to remap
            2) map_dict (dictionary): dictionary of classes and their remapped groups
                a. keys = classes, values = mapped group
                b. if not included, will retain same value (or use default_group if provided)
            3) method (set string): style of casing
                a. options: 'lower', 'upper', 'title'
            4) default_group (string, optional): the group classes unseen in training would fall into
                a. must be one of the values in the map_dict
            
        Description:
            Remaps classes to your desired groupings
        
        """
        self.variable = variable
        self.map_dict = map_dict
        
        # Check if method is valid
        valid_methods = [e.value for e in CompressClassesMethod]
        if method not in valid_methods:
            raise ValueError(f"CompressClasses: method must be one of {valid_methods}")
        self.method = method
        
        # Validate default_group if provided
        if default_group is not None:
            if default_group not in map_dict.values():
                raise ValueError(f"CompressClasses: default_group '{default_group}' must be one of the values in map_dict: {list(set(map_dict.values()))}")
        self.default_group = default_group

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"CompressClasses: variable {self.variable} not found in X")
        
        # Build normalized map dictionary based on method
        d = {}
        for k in self.map_dict.keys():
            if self.method == CompressClassesMethod.LOWER.value:
                d[str(k)] = str(self.map_dict.get(k)).lower().strip()
            elif self.method == CompressClassesMethod.UPPER.value:
                d[str(k)] = str(self.map_dict.get(k)).upper().strip()
            elif self.method == CompressClassesMethod.TITLE.value:
                d[str(k)] = str(self.map_dict.get(k)).title().strip()
        self.map_dict = d

        # Store values for inference check
        cleaned_series = X[self.variable].unique()
        cleaned_series = cleaned_series[~pd.isnull(cleaned_series)]
        self.unq_fit_values_ = list(cleaned_series)
        self.unq_fit_values_.extend(list(self.map_dict.values()))
        # remove duplicates
        self.unq_fit_values_ = list(set(self.unq_fit_values_))
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"CompressClasses: variable {self.variable} not found in X during transform")
        
        # Convert to string if numeric
        vartype = str(X[self.variable].dtype)
        if 'int' in vartype or 'float' in vartype:
            X[self.variable] = X[self.variable].astype(str)
        
        # Apply mapping
        def map_value(x):
            if pd.isnull(x):
                return x
            x_str = str(x)
            if self.method == CompressClassesMethod.LOWER.value:
                key = x_str.lower().strip()
            elif self.method == CompressClassesMethod.UPPER.value:
                key = x_str.upper().strip()
            else:  # title
                key = x_str.title().strip()
            
            if key in self.map_dict.keys():
                return self.map_dict[key]
            elif self.default_group is not None:
                return self.default_group
            else:
                return x_str
        
        X[self.variable] = X[self.variable].apply(map_value)
        
        # Warn about unmapped values if default_group not provided
        if self.default_group is None:
            # Check for values that weren't mapped
            cleaned_series = X[self.variable].unique()
            cleaned_series = cleaned_series[~pd.isnull(cleaned_series)]
            original_values = set(list(cleaned_series))
            mapped_values = set(self.unq_fit_values_)
            unmapped = original_values - mapped_values
            if unmapped:
                print(f"WARNING (CompressClasses): column '{self.variable}' has values that were not in the map_dict and no default_group was provided: {unmapped}. These values were retained as-is.")
        
        return X    

class DropVar(BaseEstimator, TransformerMixin):
    def __init__(self, variable: str):
        """
        Arguments:
            1) variable (string): name of the variable to drop

        Description:
            Drops the input variable
        
        """
        self.variable = variable
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None, **fit_params) -> Self:
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"DropVar: variable {self.variable} not found in X")
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()
        
        # Check if variable is in X
        if self.variable not in X.columns:
            raise ValueError(f"DropVar: variable {self.variable} not found in X during transform")
        
        X.drop(columns=[self.variable], inplace=True)
        return X