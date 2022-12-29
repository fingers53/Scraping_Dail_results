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
@logg
def get_dail_constituencies_all(latest_dail : int):
    
    # stores data #
    data_dail_constituencies = []
    
    # Dails to itterate through 
    latest_dail = latest_dail
    
    # All constituencies # 
    headers = {
        'accept': 'application/json',
    }
    
    for i in range(1, latest_dail+1) : 
            
        params = {
            'chamber_type': 'house',
            'chamber': 'dail',
            'house_no': str(i),
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
    
    
    # final dataframe
    df_dail_constituencies = pd.DataFrame(data_dail_constituencies)
    
    
    # used in 
    project_id= "red-bus-371614"        
    credentials = service_account.Credentials.from_service_account_file('./../secrets/big_key.json')
    
    destination_table = 'oireachtas_api.dail_constituencies'
    what_to_if_table_exists = 'replace' 
    
    upload(df_dail_constituencies,destination_table,'replace',project_id,credentials)# uploades dataframe to bigquery

    
    return df_dail_constituencies

@logg
def get_dail_members_all(latest_dail : int) :
    
    # All reps # 
     # stores data #
    data_dail_members = [] # more than the number of seats, due to bye elections
    data_dail_number_of_members = [] # will have more deputies than this due to bye elections???
    
    # Dails to itterate through 
    latest_dail = latest_dail
    
    headers = {
        'accept': 'application/json',
    }
    
    for i in range(1, latest_dail+1) : 
        
        params = {
            'chamber': 'dail',
            'house_no': str(i),
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
                  
                  if len(data[info]['memberships'][0]['membership']['represents']) == 0 :
                      represents = ""
                      representCode = ""
                      name_2nd = ""
                      uri_represents = "" 
                  else : 
                      represents = data[info]['memberships'][0]['membership']['represents']
                      representCode = data[info]['memberships'][0]['membership']['represents'][0]['represent']['representCode']
                      name_2nd = data[info]['memberships'][0]['membership']['represents'][0]['represent']['showAs']
                      uri_represents = data[info]['memberships'][0]['membership']['represents'][0]['represent']['uri']
                   
                  
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
                      'represents' : represents,
                      'representCode': representCode,
                      'name_2nd': name_2nd,
                      'uri_represents': uri_represents,
                   
                     })


        
    df_data_dail_members = (pd.DataFrame(data_dail_members)
                                    .drop(columns=['represents','memberships'])
                                    .rename(columns={'start': 'start_of_term', 
                                                       'end': 'end_of_term'})
                            )
    df_data_dail_number_of_members = pd.DataFrame(data_dail_number_of_members)
    

    # used in 
    project_id= "red-bus-371614"        
    credentials = service_account.Credentials.from_service_account_file('./../secrets/big_key.json')
    
    destination_table = 'oireachtas_api.dail_members'
    what_to_if_table_exists = 'replace' 
    
    upload(df_data_dail_members,destination_table,'replace',project_id,credentials)# uploades dataframe to bigquery

    return df_data_dail_members

@logg
def get_dail_members_party_history(latest_dail : int):
        
    #All reps # 
    # stores data #
    data_dail_members = [] # more than the number of seats, due to bye elections
    data_dail_number_of_members = [] # will have more deputies than this due to bye elections???
    list_parties = []         # for tracking parties

    party_data = []

    # Dails to itterate through 
    latest_dail = latest_dail
    
    headers = {
        'accept': 'application/json',
    }
    
    for i in range(1, latest_dail+1) : 
        
        params = {
            'chamber': 'dail',
            'house_no': str(i),
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
       

        house_no = i
        
        for data in memebers_of_dail: 
            # data is dict withing the list memebers_of_dail
            # each dict is linked to a member of the dail for a given dail term
            for info in data : 
            # info is the info in the data dict 
                member = data[info]['memberCode']
                houseCode = 'dail'
                memberships_info = data[info]['memberships'][0]['membership'] # party data
                
                for membership in memberships_info:
                    # Extract the parties list from the membership
                    parties = memberships_info['parties']
                    # Loop through the parties
                    for party in parties:
                        # Extract the party name and date range
                        party_name = party['party']['showAs']
                        date_range = party['party']['dateRange']
            
                        party_data.append((party_name, date_range,member,houseCode,house_no))
                        
                              
        
    df = pd.DataFrame(party_data, columns=['party', 'date_range','member',"houseCode","house_no"])
    df[['start', 'end']] = df['date_range'].apply(lambda x: pd.Series({'start': x['start'], 'end': x['end']}))
    
    # Dataframe to be uploaded 
    df = df[['party', 'member', 'start', 'end',"houseCode","house_no"]].drop_duplicates()
        
     # used in 
    project_id= "red-bus-371614"        
    credentials = service_account.Credentials.from_service_account_file('./../secrets/big_key.json')
    
    destination_table = 'oireachtas_api.dail_members_party_history'
    what_to_if_table_exists = 'replace' 
    
    upload(df,destination_table,'replace',project_id,credentials)# uploades dataframe to bigquery

    return df

if __name__ == "__main__":
    a = get_dail_constituencies_all(33)
    aa = get_dail_members_all(33)
    aaa = get_dail_members_party_history(33)
    
