import env
import pandas as pd 
from os import path 
import os

def get_connection(database):
    '''
    Database: string; name of database that the url is being created for
    '''
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{database}'

sql_query = '''
SELECT prop.*, logerror, transactiondate, airconditioningdesc, architecturalstyledesc, buildingclassdesc, heatingorsystemdesc, propertylandusedesc, storydesc, typeconstructiondesc
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
	ON prop.parcelid = subq.parcelid
LEFT JOIN airconditioningtype
	ON prop.airconditioningtypeid = airconditioningtype.airconditioningtypeid
LEFT JOIN architecturalstyletype
	ON prop.architecturalstyletypeid = architecturalstyletype.architecturalstyletypeid
LEFT JOIN buildingclasstype 
	ON prop.buildingclasstypeid = buildingclasstype.buildingclasstypeid
LEFT JOIN heatingorsystemtype
	ON prop.heatingorsystemtypeid = heatingorsystemtype.heatingorsystemtypeid
LEFT JOIN propertylandusetype
	ON prop.propertylandusetypeid = propertylandusetype.propertylandusetypeid
LEFT JOIN storytype
	ON prop.storytypeid = storytype.storytypeid
LEFT JOIN typeconstructiontype
	ON prop.typeconstructiontypeid = typeconstructiontype.typeconstructiontypeid
WHERE latitude IS NOT NULL 
	AND longitude IS NOT NULL
	AND transactiondate BETWEEN '2017-01-01' AND '2017-12-31';
'''

def acquire_cache_zillow():
    if not path.isfile('zillow.csv'):
        url = get_connection('zillow')
        zillow = pd.read_sql(sql_query, url)
        zillow.to_csv('zillow.csv')
    zillow = pd.read_csv('zillow.csv')
    zillow.drop(columns=['Unnamed: 0'], inplace=True)
    return zillow

