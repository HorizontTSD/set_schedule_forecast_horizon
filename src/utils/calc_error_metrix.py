import numpy as np
import pandas as pd

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def mean_absolute_percentage_error(y_true, y_pred):
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


def symmetric_mean_absolute_percentage_error(y_true, y_pred):
    return 100 * np.mean(2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred)))


def normalized_root_mean_squared_error(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred)) / (np.max(y_true) - np.min(y_true))


def mean_absolute_range_normalized_error(y_true, y_pred):
    return mean_absolute_error(y_true, y_pred) / (np.max(y_true) - np.min(y_true))


def mean_absolute_scaled_error(y_true, y_pred):
    naive_forecast = y_true.shift(1).dropna()
    return mean_absolute_error(y_true[1:], y_pred[1:]) / mean_absolute_error(y_true[1:], naive_forecast)


def weighted_mean_absolute_percentage_error(y_true, y_pred):
    return np.sum(np.abs(y_true - y_pred)) / np.sum(np.abs(y_true)) * 100


import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def symmetric_mean_absolute_percentage_error(y_true, y_pred):
    return 100 * np.mean(2 * np.abs(y_true - y_pred) / (np.abs(y_true) + np.abs(y_pred)))

def normalized_root_mean_squared_error(y_true, y_pred):
    return np.sqrt(np.mean((y_true - y_pred)**2)) / (np.max(y_true) - np.min(y_true))

def mean_absolute_range_normalized_error(y_true, y_pred):
    return np.mean(np.abs(y_true - y_pred)) / (np.max(y_true) - np.min(y_true))

def weighted_mean_absolute_percentage_error(y_true, y_pred):
    return 100 * np.sum(np.abs(y_true - y_pred)) / np.sum(np.abs(y_true))

def metrix_all(col_time, col_target, df_evaluetion, df_comparative):
    df_evaluetion[col_time] = pd.to_datetime(df_evaluetion[col_time])
    df_comparative[col_time] = pd.to_datetime(df_comparative[col_time])

    df_evaluetion = df_evaluetion.sort_values(col_time).reset_index(drop=True)
    df_comparative = df_comparative.sort_values(col_time).reset_index(drop=True)

    df_merged = pd.merge_asof(
        df_evaluetion,
        df_comparative,
        on=col_time,
        direction='nearest',
        suffixes=('_pred', '_true')
    )

    df_merged['MAE'] = np.abs(df_merged[f'{col_target}_true'] - df_merged[f'{col_target}_pred'])
    df_merged['RMSE'] = np.sqrt((df_merged[f'{col_target}_true'] - df_merged[f'{col_target}_pred'])**2)
    df_merged['MAPE'] = np.abs((df_merged[f'{col_target}_true'] - df_merged[f'{col_target}_pred']) / df_merged[f'{col_target}_true']) * 100

    df_merged = df_merged.rename(columns={f"{col_target}_true": col_target})


    return df_merged