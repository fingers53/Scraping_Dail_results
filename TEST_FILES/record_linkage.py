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


# Functions 
def get_year_from_date_string(date_str):
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

def get_election_type_from_string(election_type_str):
    if 'Town' in election_type_str or 'Local' in election_type_str :
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

def was_elected(status):
    status = str(status) # some of the status are None 

    if status == 'Elected':
        return True
    elif status == 'Not Elected':
        return False

    else: # for the rows that represent resignations or appointments or some other event in a politicans career
        return None

def fix_party(main,column_name) : 
    
    
    
    return 
    
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



df_electionsireland = (pd.read_parquet(file_name_electionsireland)
                       .rename(columns={'ID':'candidate_ID'}) # rename columns
                       .drop(columns=['seat','count_eliminated']) # drop columns
                       .reset_index() # reset index
                       .drop(columns=['index']) # drop column
                       #.pipe(get_year_from_date_string,'year') one day would have this logic in a function that can use .pipe
                       )

df_electionsireland['year'] = (df_electionsireland
                               .date
                               .apply(get_year_from_date_string)
                               )
df_electionsireland = df_electionsireland.drop(columns=['date']) # drop column
df_electionsireland['election_type'] = (df_electionsireland
                                        .election_type
                                        .apply(get_election_type_from_string)
                                        )
df_electionsireland['elected'] = (df_electionsireland
                                  .status
                                  .apply(was_elected)
                                  )


# Ground truth
historic_constituencies = pd.read_html('https://en.wikipedia.org/wiki/Historic_D%C3%A1il_constituencies',flavor='bs4')[1]
current_constituencies = pd.read_html('https://en.wikipedia.org/wiki/D%C3%A1il_constituencies',flavor='bs4')[3]

historic_elections = pd.read_html('https://en.wikipedia.org/wiki/Elections_in_the_Republic_of_Ireland',flavor='bs4')[3]





"""
df_irelandelection = (pd.read_parquet(file_name_irelandelection)
                      .rename(
                         columns={'first_pref_quota_ratio':'pct_of_quota_reached_with_first_pref',
                                  'constituency':'constituency_name'}
                         )
                      .reset_index()
                      .drop(columns=['index'])
                      )
"""