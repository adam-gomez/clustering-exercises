import env
import pandas as pd 
from os import path 
import os

def get_connection(database):
    '''
    Database: string; name of database that the url is being created for
    '''
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{database}'

def acquire_cache_zillow():
    if not path.isfile('zillow.csv'):
        query = '''
        SELECT prop.*, logerror, transactiondate
        FROM properties_2017 as prop 
        INNER JOIN (
	        SELECT id, p.parcelid, logerror, transactiondate
	        FROM predictions_2017 AS p
	        INNER JOIN (
	        	SELECT parcelid,  MAX(transactiondate) AS max_date
	        	FROM predictions_2017 
	        	GROUP BY (parcelid)) AS sub
	        		ON p.parcelid = sub.parcelid
	        	WHERE p.transactiondate = sub.max_date
        		) AS subq
        	ON prop.id = subq.id
        WHERE latitude IS NOT NULL 
	        AND longitude IS NOT NULL
	        AND transactiondate BETWEEN '2017-01-01' AND '2017-12-31'
        '''
        url = get_connection('zillow')
        zillow = pd.read_sql(query, url)
        zillow.to_csv('zillow.csv')
    zillow = pd.read_csv('zillow.csv')
    zillow.drop(columns=['Unnamed: 0'], inplace=True)
    return zillow

sql_query = '''
SELECT prop.*, logerror, transactiondate
FROM properties_2017 as prop 
INNER JOIN (
	SELECT id, p.parcelid, logerror, transactiondate
	FROM predictions_2017 AS p
	INNER JOIN (
		SELECT parcelid,  MAX(transactiondate) AS max_date
		FROM predictions_2017 
		GROUP BY (parcelid)) AS sub
			ON p.parcelid = sub.parcelid
		WHERE p.transactiondate = sub.max_date
		) AS subq
	ON prop.id = subq.id
WHERE latitude IS NOT NULL 
	AND longitude IS NOT NULL
	AND transactiondate BETWEEN '2017-01-01' AND '2017-12-31'
'''