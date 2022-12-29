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
#import pandas_gbq



# dealing with non relative paths
import pathlib
from pathlib import Path

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


# Functions 
 
@logg
def get_election_type(data,colum):
    
    def get_election_type_from_string(election_type_str):
        if election_type_str is None:
            return None
        elif 'Town' in election_type_str or 'Local' in election_type_str :
            return 'LOCAL'
        elif 'Dail' in election_type_str:
            return 'GENERAL'
        elif 'Seanad' in election_type_str:
            return 'SEANAD'
        elif 'Westminster' in election_type_str:
            return 'Westminster'.upper()
        elif 'European' in election_type_str:
            return 'EUROPEAN'
        elif 'By Election' in election_type_str:
            return 'BI-ELECTION'
        else: # for the rows that represent resignations or appointments or some other event in a politicans career
            return None
        
    data[str(colum)] = data['election_type'].apply(get_election_type_from_string)
    
    return data
  
@logg
def was_elected(data,colum):
    
    def check_status(status):
        if status == 'Elected':
            return True
        elif status == 'Not Elected':
            return False
        else: # for the rows that represent resignations or appointments or some other event in a politicans career
            return None
    
    # create column
    data[str(colum)] = data['status'].apply(check_status)
    
    return data

@logg
def get_year_from_date_string(main):
    
    
    # couldnt work how to get this to work without using apply
    def get_year_from_date_string_1(date_str):
        if date_str == None:
            return 0
        elif len(date_str) > 4:
            try:
                return int(date_str[-4:])
            except:
                if isinstance(date_str[-1],int):#last letter is an int
                    return date_str[-4:]
        else:
            return int(date_str)
        
    main['year'] = (main
                   .date
                   .apply(get_year_from_date_string_1)
                   )

    return main
    
"""
Tables we want : 
    Number of total elections ? 
    Number of total candaites ? 
    Coverage per website i.e. how many elections does each site have
        - How many people ran per election 
        - How many 
            -> constituencies
            -> how many elected reps 
            -> how many reps 
"""

# Files # 
file_name_irelandelection = "../irelandelection/ALL_CANDIDATES.parquet"
file_name_electionsireland = '../electionsireland_data/ElectionsIreland_candidate.parquet'


# df_electionsirelan 
# Cleaning # 
# 1. Rename columns : rename(columns={'ID':'candidate_ID'})
# 2. Drop columns : .drop(columns=['seat','count_eliminated'])
# 3. get_year_from_date_string : date.apply(get_year_from_date_string)

df_electionsireland = (pd.read_parquet(file_name_electionsireland)
                       .rename(columns={'ID':'candidate_ID'}) # rename columns
                       .drop(columns=['seat','count_eliminated']) # drop columns
                       .reset_index() # reset index
                       .drop(columns=['index']) # drop column
                       .pipe(get_year_from_date_string) # get year of election
                       .drop(columns=['date'])  # drop column
                       .pipe(was_elected,"elected") # create column that returns True or False if someone was elected or not
                       .pipe(get_election_type,"election_type") # extract the election type 
                       )




# df_irelandelection
# Cleaning # 
# 1. Rename columns : .rename( columns={'first_pref_quota_ratio':'pct_of_quota_reached_with_first_pref', 'constituency':'constituency_name'} )

df_irelandelection = (pd.read_parquet(file_name_irelandelection)
                      .rename(
                         columns={'first_pref_quota_ratio':'pct_of_quota_reached_with_first_pref',
                                  'constituency':'constituency_name'}
                         )
                      .reset_index()
                      .drop(columns=['index'])
                      )



