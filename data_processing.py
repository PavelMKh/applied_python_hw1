from joblib import Parallel, delayed
import pandas as pd
from datetime import datetime

def clean_data(df): 
    mask = Parallel(n_jobs=-1)(delayed(filter_city)(row) for index, row in df.iterrows())
    df = df[mask]
    
    mean_std = df.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    df = df.merge(mean_std, on=['city', 'season'])

    df['is_outlier_2s'] = (df['temperature'] > (df['mean'] + 2 * df['std'])) | \
                (df['temperature'] < (df['mean'] - 2 * df['std']))

    return df

def compute_outliers_2s(temp, upper_bound, lower_bound):
        return (temp > upper_bound) | (temp < lower_bound)

def filter_city(row):
    return not (row['city'] in ['Sydney', 'Singapore', 'Rio de Janeiro'] and pd.to_datetime(row['timestamp']) <= pd.to_datetime('2010-01-30'))

def get_season(current_month):
    if current_month in [12, 1, 2]:
        return "winter"
    elif current_month in [3, 4, 5]:
        return "spring"
    elif current_month in [6, 7, 8]:
        return "summer"
    elif current_month in [9, 10, 11]:
        return "autumn"

def get_stats_by_season(df, selected_city):
    current_month = datetime.now().month
    current_season = get_season(current_month)
    stats_by_season = df.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    mean_t_season = stats_by_season[(stats_by_season['city'] == selected_city) & 
                                    (stats_by_season['season'] == current_season)]['mean']
    std_t_season = stats_by_season[(stats_by_season['city'] == selected_city) & 
                                   (stats_by_season['season'] == current_season)]['std']
    return mean_t_season.values[0], std_t_season.values[0]



def get_stats_by_month(df, selected_city):
    current_month = datetime.now().month
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['month'] = df['timestamp'].dt.month
    df = df.groupby(['city', 'month'])['temperature'].agg(['mean', 'std']).reset_index()
    mean_t_month = df[(df['city'] == selected_city) & 
                                    (df['month'] == current_month)]['mean']
    std_t_month = df[(df['city'] == selected_city) & 
                                    (df['month'] == current_month)]['std']
    return mean_t_month.values[0], std_t_month.values[0]

def is_normal_weather(mean, std, temp):
    return abs(temp - mean) <= 2 * std