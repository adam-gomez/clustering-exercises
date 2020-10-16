import env
import pandas as pd 
from os import path 
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, QuantileTransformer, PowerTransformer, RobustScaler, MinMaxScaler

def get_connection(database):
    '''
    Database: string; name of database that the url is being created for
    '''
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{database}'

sql_query = '''
SELECT * FROM customers
'''

def acquire_cache_mall():
    if not path.isfile('mall.csv'):
        url = get_connection('mall_customers')
        mall = pd.read_sql(sql_query, url)
        mall.to_csv('mall.csv')
    mall = pd.read_csv('mall.csv')
    mall.drop(columns=['Unnamed: 0'], inplace=True)
    return mall

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .75):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

def split_my_data(df, pct=0.10):
    '''
    This divides a dataframe into train, validate, and test sets. 

    Parameters - (df, pct=0.10)
    df = dataframe you wish to split
    pct = size of the test set, 1/2 of size of the validate set

    Returns three dataframes (train, validate, test)
    '''
    train_validate, test = train_test_split(df, test_size=pct, random_state = 123)
    train, validate = train_test_split(train_validate, test_size=pct*2, random_state = 123)
    return train, validate, test

def split_stratify_my_data(df, strat, pct=0.10):
    '''
    This divides a dataframe into train, validate, and test sets straifying on the selected feature

    Parameters - (df, pct=0.10, stratify)
    df = dataframe you wish to split
    pct = size of the test set, 1/2 of size of the validate set
    stratify = a string of the column name of the feature you wish to stratify on

    Returns three dataframes (train, validate, test)
    '''
    train_validate, test = train_test_split(df, test_size=pct, random_state = 123, stratify=df[strat])
    train, validate = train_test_split(train_validate, test_size=pct*2, random_state = 123, stratify=train_validate[strat])
    return train, validate, test

def min_max_scaler(train, validate, test):
    '''
    Accepts three dataframes and applies a linear transformer to convert values in each dataframe
    to a value from 0 to 1 while mantaining relative distance between values. 
    Columns containing object data types are dropped, as strings cannot be directly scaled.

    Parameters (train, validate, test) = three dataframes being scaled
    
    Returns (scaler, train_scaled, validate_scaled, test_scaled)
    '''
    train = train.select_dtypes(exclude=['object'])
    validate = validate.select_dtypes(exclude=['object'])
    test = test.select_dtypes(exclude=['object'])    
    scaler = MinMaxScaler(copy=True, feature_range=(0,1)).fit(train)
    train_scaled = pd.DataFrame(scaler.transform(train), columns=train.columns.values).set_index([train.index.values])
    validate_scaled = pd.DataFrame(scaler.transform(validate), columns=validate.columns.values).set_index([validate.index.values])
    test_scaled = pd.DataFrame(scaler.transform(test), columns=test.columns.values).set_index([test.index.values])
    return scaler, train_scaled, validate_scaled, test_scaled 