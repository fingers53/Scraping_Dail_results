import os
import sys
import datetime as dt
import functools
from os import listdir
from os.path import isfile, join
import pathlib

# Libs 
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from pandas.io import gbq
from functools import wraps

from functools import wraps
from google.oauth2 import service_account
import pandas_gbq

import seaborn as sns


# dealing with non relative paths
import pathlib
from pathlib import Path

# used in 
project_id= "red-bus-371614"        
credentials = service_account.Credentials.from_service_account_file('./../secrets/big_key.json')
    


# Base 
def logg(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		tic = dt.datetime.now()
		result = func(*args, **kwargs)
		time_taken = str(dt.datetime.now() - tic)
		try:
			print(f"just ran step {func.__name__} shape={result.shape} took {time_taken}s")
		except:
			print(f"just ran step {func.__name__} took {time_taken}s")
		return result
	return wrapper

@logg
def download(dataset,table_name,project_id,credentials):
    #project_id = project_id
    #sql query 
    sql_query = """SELECT * FROM `""" + str(project_id) + """."""+ str(dataset) +""".""" +str(table_name)+"""`  """
    
    # The dataframe version of that table 
    main = pandas_gbq.read_gbq(sql_query, project_id= project_id, credentials=credentials)
    
    return  main

@logg
def upload(dataframe,destination_table,what_to_if_table_exists,project_id,credentials):
    # uploads tables to bigquery 
    
    pandas_gbq.to_gbq(dataframe,
        destination_table = destination_table,
        project_id = project_id ,
        if_exists = what_to_if_table_exists , #'replace','append',  # 'fail' is for new
        credentials=credentials
        
        )
    print("\nUploaded to "+ str(destination_table))
    
    return


"""
    irelandelection
"""

file_name_irelandelection = "../irelandelection/ALL_CANDIDATES.parquet"


df_irelandelection = (pd.read_parquet(file_name_irelandelection)
                      .rename(
                         columns={'first_pref_quota_ratio':'pct_of_quota_reached_with_first_pref',
                                  'constituency':'constituency_name'}
                         )
                      .reset_index()
                      .drop(columns=['index'])
                      )

### Other 
'date', 'election_type', 'party', 'status', 'constituency_name',
'first_pref_count', 'first_pref_pct',
'pct_of_quota_reached_with_first_pref', 'ran_unopposed', 'candidate',
'candidate_ID', 'year', 'Year_', 'Month', 'Day', 'elected'
### This 
'election', 'elected', 'party', 'first_pref_pct', 'first_pref_count',
'pct_of_quota_reached_with_first_pref', 'year', 'candidate',
'constituency_name', 'election_type'


# elected # (status)
"""
False, True
"""

# election_type # 
"""
, 'LOCAL'
-> No SEANAD data
"""

### GE ###
#values_all =


values_2020 = (df_irelandelection[~df_irelandelection.year.isin([2019])]
               .query("election_type != ['BI-ELECTION', 'EUROPEAN', 'LOCAL']") # remove seanad election
                # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") 
               
               )

### LE ###
"""
    - 2019 : 
"""
values_locals = (df_irelandelection
               .query("election_type != ['BI-ELECTION', 'EUROPEAN', 'GENERAL']") # remove seanad election
               #.query("candidate_ID != '3694'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 
              
               )
