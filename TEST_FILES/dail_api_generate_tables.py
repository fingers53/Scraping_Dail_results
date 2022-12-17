# Built-in/Generic Imports
import os
import sys
import glob
import datetime as dt
import requests
from pathlib import Path
import pathlib

# Libs 
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from pandas.io import gbq
from functools import wraps
import pandas_gbq

import os.path

import yaml
import gspread
from gspread import authorize
from google.oauth2 import service_account


import requests
import json
import matplotlib.pyplot as plt


"""
Functions that are useful for this repo 
"""
### Usual Logging functions for intermal loggin 

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

### Bigquery functions ###  ALL REPEATS 
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



    
##############################################################################
def get_dail_constituencies_all():
    
    # stores data #
    data_dail_constituencies = []
    
    # Dails to itterate through 
    latest_dail = 33
    lst = [str(i) for i in range(1, latest_dail+1)]
    
    # All constituencies # 
    headers = {
        'accept': 'application/json',
    }
    
    for i in lst : 
            
        params = {
            'chamber_type': 'house',
            'chamber': 'dail',
            'house_no': i,
            'memberCode':"" ,
            
            'limit': '1000',
        }
        
        dail_constituencies_response = (requests
                    .get('https://api.oireachtas.ie/v1/constituencies', params=params, headers=headers)
                    .json()['results']
                    )
        
        
        dail_name = dail_constituencies_response["house"]["house"]
        dail_constituencies  = dail_constituencies_response["house"]["constituenciesOrPanels"]
        
        
        for data in dail_constituencies: 
            for info in data : 
                
                 data_dail_constituencies.append({
                'houseCode' : dail_name["houseCode"],
                'houseNo' : dail_name["houseNo"],
                'item': data[info]["representType"],
                'name': data[info]["representCode"],
                'uri': data[info]["uri"],
                'name_2nd': data[info]["showAs"],
                    })
    
    
    # 
    df_dail_constituencies = pd.DataFrame(data_dail_constituencies)
    
    
    
    return df_dail_constituencies


#df_dail_constituencies = get_dail_constituencies_all()

"""
# used in 
project_id= config['IE']['project_id']         
credentials = service_account.Credentials.from_service_account_file('../../secrets/big_key.json')

dataset = department
destination_table = str(dataset)+ '.' + str(config['IE']['Tables'][department][report_dataset]['BIGQUERY_TABLE']) # syntaca : data_set.table_name
what_to_if_table_exists = 'replace' 

upload(data_frame_spreadsheet_info,destination_table,'replace',project_id,credentials)# uploades dataframe to bigquery
"""

"""
Stats I want 
 - Number of constituencies per dail 
"""
# All reps # 
 # stores data #
data_dail_members = [] # more than the number of seats, due to bye elections
data_dail_number_of_members = [] # will have more deputies than this due to bye elections???
data_dail_members_changed_party = [] # more than the number of seats, due to bye elections

# Dails to itterate through 
latest_dail = 33
lst = [str(i) for i in range(1, latest_dail+1)]

headers = {
    'accept': 'application/json',
}
for i in lst: 
    
    params = {
        'chamber': 'dail',
        'house_no': i,
        'limit': '1000',
    }
    
    response = requests.get('https://api.oireachtas.ie/v1/members', params=params, headers=headers).json()
    
    # Total number of memebers per dail 
    total_memebers_per_dail = response["head"]['counts']['memberCount']
    data_dail_number_of_members.append({
                    'houseCode' : 'dail',
                    'house_no' : i, # the value being itterateed
                    'memberCount': total_memebers_per_dail,
                    
                        })
    
    # The memebers that make up each dail 
    memebers_of_dail  = response['results']
   
     
    for data in memebers_of_dail:  # data is a dict of size 10 
          for info in data : 
              
              parties = data[info]['memberships'][0]['membership']['parties']
              parties_list = []
              for i in parties:
                  parties_list.append(i)
              
              data_dail_members.append({
                  'memberCode': data[info]['memberCode'],
                  'firstName': data[info]['firstName'] ,
                  'lastName': data[info]['lastName'], 
                  'gender': data[info]['gender'], 
                  'dateOfDeath': data[info]['dateOfDeath'], 
                  'fullName': data[info]['fullName'], 
                  'pId': data[info]['pId'], 
                  'wikiTitle': data[info]['wikiTitle'], 
                  'uri': data[info]['uri'], 
                  'memberships': data[info]['memberships'][0]['membership'],
                  'houseCode': data[info]['memberships'][0]['membership']['house']['houseCode'],
                  'houseNo': data[info]['memberships'][0]['membership']['house']['houseNo'],
                  'start': data[info]['memberships'][0]['membership']['dateRange']['start'],
                  'end' : data[info]['memberships'][0]['membership']['dateRange']['end'],
                  'parties' : parties_list[0] , #data[info]['memberships'][0]['membership']['parties'],
                  'represents' : data[info]['memberships'][0]['membership']['represents'],
                  'representCode': data[info]['memberships'][0]['membership']['represents'][0]['represent']['representCode'],
                  'name_2nd': data[info]['memberships'][0]['membership']['represents'][0]['represent']['showAs'],
                  'uri_represents': data[info]['memberships'][0]['membership']['represents'][0]['represent']['uri'],
               
                 })
              
                  
                  
              if len(parties_list) != 1 : 
                  print("yes")
                  print(len(parties_list))
                  
                  data_dail_members_changed_party.append({
                  'memberCode': data[info]['memberCode'],
                  'firstName': data[info]['firstName'] ,
                  'lastName': data[info]['lastName'], 
                  'gender': data[info]['gender'], 
                  'dateOfDeath': data[info]['dateOfDeath'], 
                  'fullName': data[info]['fullName'], 
                  'pId': data[info]['pId'], 
                  'wikiTitle': data[info]['wikiTitle'], 
                  'uri': data[info]['uri'], 
                  'memberships': data[info]['memberships'][0]['membership'],
                  'houseCode': data[info]['memberships'][0]['membership']['house']['houseCode'],
                  'houseNo': data[info]['memberships'][0]['membership']['house']['houseNo'],
                  'start': data[info]['memberships'][0]['membership']['dateRange']['start'],
                  'end' : data[info]['memberships'][0]['membership']['dateRange']['end'],
                  'parties' : parties_list[1] , #data[info]['memberships'][0]['membership']['parties'],
                  'represents' : data[info]['memberships'][0]['membership']['represents'],
                  'representCode': data[info]['memberships'][0]['membership']['represents'][0]['represent']['representCode'],
                  'name_2nd': data[info]['memberships'][0]['membership']['represents'][0]['represent']['showAs'],
                  'uri_represents': data[info]['memberships'][0]['membership']['represents'][0]['represent']['uri'],
                  'number_of_parties':parties_list,
                 })
                 
              else : 
                  pass
              

    
df_data_dail_members = pd.DataFrame(data_dail_members)
df_data_dail_number_of_members = pd.DataFrame(data_dail_number_of_members)





def get_dail_members_all():
    
    
    
    return df_dail_constituencies
