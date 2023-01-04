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

@logg
def column_transformation_raw(dataframe):
    """
    Carries out the required transfromations such that the columns of the 
    dataframes can be uploaded to bigquery 
    
    replacing:
        ' ' -> _
        ()  ->''

    Parameters
    ----------
    dataframe : dataframe
     dataframe being uploaded to bigquery 

    Returns
    -------
    None.

    """
    # used in 
    # RAW_Upload.py
    
    
    
    dataframe.columns = (dataframe.columns
                        .str.replace(' ', '_')
                        .str.replace('(', '')
                        .str.replace(')', '')
                        )
    
    return 

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

@logg
def get_date_from_date_string(main): 
    
    def extract_date(date_str):
        if date_str is None:
            # Return None for year, month, and day if the input is None
            return None, None, None
        try:
            # Try to parse the date using the default date parser
            date = dt.datetime.strptime(date_str, "%d %B %Y")
            year = date.year
            month = date.month
            day = date.day
        except ValueError:
            # If the date is not in the expected format, try to parse just the year
            try:
                # Try to parse the date using the default date parser
                date = dt.datetime.strptime(date_str, "%B %Y")
                year = date.year
                month = date.month
                day = None
            
            except ValueError:
                try:
                    year = int(date_str)
                    month = None
                    day = None
                except ValueError:
                    # If the year cannot be parsed, the date is invalid
                    year = None
                    month = None
                    day = None
        return year, month, day

    main["Parsed Date"] = main["date"].apply(extract_date)
    
    main["Year_"] = main["Parsed Date"].apply(lambda x: x[0])
    main["Month"] = main["Parsed Date"].apply(lambda x: x[1])
    main["Day"] = main["Parsed Date"].apply(lambda x: x[2])


    return main

@logg
def clean_date_column(main): 
    month_names = {
    "Jan": "January",
    "Feb": "February",
    "Mar": "March",
    "Apr": "April",
    "May": "May",
    "Jun": "June",
    "Jul": "July",
    "Aug": "August",
    "Sep": "September",
    "Oct": "October",
    "Nov": "November",
    "Dec": "December",
    }

    # Define a function that replaces the short month names with the full month names using the dictionary
    def expand_month(date_str):
        if date_str is None:
            return None
        # Replace each short month name with the corresponding full month name
        for short_name, full_name in month_names.items():
            date_str = date_str.replace(short_name, full_name)
        return date_str
    
    # Apply the expand_month function to the "Date" column
    main["date"] = main["date"].apply(expand_month)

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
file_name_electionsireland = '../electionsireland_data/ElectionsIreland_candidate.parquet'


# df_electionsirelan  : df1
# Cleaning # 
# 1. Rename columns : rename(columns={'ID':'candidate_ID'})
# 2. Drop columns : .drop(columns=['seat','count_eliminated'])
# 3. get_year_from_date_string : date.apply(get_year_from_date_string)

df_electionsireland = (pd.read_parquet(file_name_electionsireland)
                       .rename(columns={'ID':'candidate_ID'}) # rename columns
                       .drop(columns=['seat','count_eliminated']) # drop columns
                       .reset_index() # reset index
                       .drop(columns=['index']) # drop column
                       .pipe(clean_date_column)
                       .pipe(get_year_from_date_string) # get year of election
                       .pipe(get_date_from_date_string)
                       .drop(columns=['Parsed Date'])  # drop column
                       .pipe(was_elected,"elected") # create column that returns True or False if someone was elected or not
                       .pipe(get_election_type,"election_type") # extract the election type 
                       .astype({'elected': str})

                       )



################## Cleaning/Processing Columns ###############
# ran_unopposed # 
"""
    - Applies more to 
"""

# elected #
elected_bad_data = ['None'] # non election event (change of party, appointment to a role, ...)



# constituency_name # 
"""
    - Includes the names of dail,eu, local and other constituency names
    - Also includes the names of roles 
"""
constituency_bad_data = [
'(Replaced  Proinsias de Rossa)',
'(Replaced  Richie Ryan)',
'(Replaced  Willie P Farrell)',
'(Replaced by  Billy Gogarty)',
'(Replaced by  Una Sheridan)',
'Administrative Panel',
'Agricultural Panel',
'Attorney General',
'Cathaoirleach (Seanad Speaker)',
'Ceann Comhairle (Speaker)',
'Chairman of the Provisional Government',
'Chief Justice',
'Chief Whip',
'Constituency TD',
'Crown Steward and Bailiff of the Manor of Northstead',
'Crown Steward and Bailiff of the three Chiltern Hundreds of Stoke, Desborough and Burnham',
'Cultural and Educational Panel',
'28460',
'EU Commission President',
'EU Commissioner for Administrative Affairs, Audit and Anti-Fraud',
'EU Commissioner for Agriculture and Rural Development',
'EU Commissioner for Competition',
'EU Commissioner for Development and Humanitarian Aid',
'EU Commissioner for Economic and Monetary Affairs',
'EU Commissioner for Education, Training, Culture and Multilingualism',
'EU Commissioner for Employment, Social Affairs and Equal Opportunities',
'EU Commissioner for Energy',
'EU Commissioner for Enlargement',
'EU Commissioner for Enterprise and Industry',
'EU Commissioner for Environment',
'EU Commissioner for External Relations and European Neighbourhood Policy',
'EU Commissioner for Financial Programming and Budget',
'EU Commissioner for Fisheries and Maritime Affairs',
'EU Commissioner for Health and Consumer Protection',
'EU Commissioner for Information Society and Media',
'EU Commissioner for Institutional Relations and Communication Strategy',
'EU Commissioner for Internal Market and Services',
'EU Commissioner for Justice, Freedom and Security',
'EU Commissioner for Regional Policy',
'EU Commissioner for Science and Research',
'EU Commissioner for Taxation and Customs Union',
'EU Commissioner for Trade',
'EU Commissioner for Transport',
'European Court of Auditors',
'First Minister of Northern Ireland',
'Industrial and Commercial Panel',
'Inishowen',
'Inner City North',
'Inner City South',
'Inner City South East',
'Inner City South West',
'Ireland',
'26665',
'Labour Panel',
'Leas Cathaoirleach (Deputy Seanad Speaker)',
'MEP',
'26724',
'Miltown Malbay',
'Minister for Agriculture and Food',
'Minister for Arts, Sport and Tourism',
'Minister for Business, Enterprise and Innovation',
'Minister for Communications, Energy and Natural Resources',
'Minister for Communications, Marine and Natural Resources',
'Minister for Community, Rural and Gaeltacht Affairs',
'Minister for Defence',
'Minister for Education and Science',
'Minister for Enterprise, Trade and Employment',
'Minister for Enterprise, Trade and Innovation',
'Minister for Environment, Heritage and Local Government',
'Minister for External Affairs',
'Minister for External Affairs [ref]',
'Minister for Finance',
'Minister for Foreign Affairs',
'Minister for Foreign Affairs [ref]',
'Minister for Health and Children',
'Minister for Industry and Commerce',
'Minister for Justice and Equality',
'Minister for Justice and Law Reform',
'Minister for Justice, Equality and Law Reform',
'Minister for Social Protection',
'Minister for Social and Family Affairs',
'Minister for Tourism, Culture and Sport',
'Minister for Transport',
'Minister for Transport and the Marine',
'Minister of State for Environmental Protection',
'Nominated by President of Executive Council',
'Nominated by Taoiseach',
'President of the Executive Council',
'Tanaiste',
'Taoiseach'
]

con_bad = df_electionsireland[df_electionsireland['constituency_name'].isin(constituency_bad_data)]
# status #
"""
     - This table includes roles people get once elected. 
     - Will filter into election and post election
"""
status_bad_data = ['(Replaced  David McKenna)', '(Replaced  Michael Flynn)',
       'Administrative Panel', 'Agricultural Panel', 'Appointed',
       'Ballybrack   -   Resigned\n            \n              (ill health)',
       'Candidate', 'Co-opted', 'Died in office:', 'Disqualified', 'Drumcliff',
       'Dublin', 'Dublin University (Trinity College)',
       'Gorey   -   Resigned', 'Inishowen',
       'Kilkenny   -   Resigned\n            \n              (dual mandate TD)',
       'Lucan', 'Nominated by Taoiseach', 'Resigned',
       'awaiting update', 'changed to']

status_bad = df_electionsireland[df_electionsireland['status'].isin(status_bad_data)]


# election_type # 
"""
    - For some years with election there are 
    - None : could be due to changing party (see 8969 ID for example [ 2009 SF -> IND])
"""

### GE ###
values_all = (df_electionsireland[df_electionsireland.year.isin([1987.0,1989.0,1992.0,1997.0,2002.0,2007.0,2011.0,2016.0,2020.0,])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL','WESTMINSTER']") # remove seanad election
               .query("candidate_ID != '3694'") # remove Ceann Comhairle 
               .query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 
               )



values_2020 = (df_electionsireland[df_electionsireland.year.isin([2020.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL']") # remove seanad election
               .query("candidate_ID != '3694'") # remove Ceann Comhairle 
               .query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )

values_2016 = (df_electionsireland[df_electionsireland.year.isin([2016.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL']") # remove seanad election
               .query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )

values_2011 = (df_electionsireland[df_electionsireland.year.isin([2011.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL']") # remove seanad election
                # remove appointments 
               .query("status != ['Appointed','awaiting update','Resigned','None']") # remove Ceann Comhairle 
               #.query("party == ['Labour']") # remove Ceann Comhairle 

               )

values_1982 = (df_electionsireland[df_electionsireland.year.isin([1982.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL']") # remove seanad election
                # remove appointments 
               .query("status != ['Appointed','awaiting update','Resigned','None']") # remove Ceann Comhairle 

               )

### LE ###
"""
    - 2019 : missing a lot of consituencies
"""
values_2019_locals = (df_electionsireland[df_electionsireland.year.isin([2019.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'GENERAL','WESTMINSTER']") # remove seanad election
               #.query("candidate_ID != '3694'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )

values_2014_locals = (df_electionsireland[df_electionsireland.year.isin([2014.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'GENERAL','WESTMINSTER']") # remove seanad election
               #.query("candidate_ID != '3694'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )


values_2009_locals = (df_electionsireland[df_electionsireland.year.isin([2009.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'EUROPEAN', 'GENERAL','WESTMINSTER']") # remove seanad election
               #.query("candidate_ID != '3694'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )


### EU ###
values_2019_EU= (df_electionsireland[df_electionsireland.year.isin([2019.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'LOCAL', 'GENERAL','WESTMINSTER']") # remove seanad election
               #.query("candidate_ID != '3694'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )

### President ###
values_presidential_election = (df_electionsireland#[df_electionsireland.year.isin([2011.0])]
               .query("election_type != ['SEANAD','BI-ELECTION', 'LOCAL', 'GENERAL','WESTMINSTER']") # remove seanad election
               .query("constituency_name == 'Ireland'") # remove Ceann Comhairle 
               #.query("status != ['Appointed','awaiting update','Resigned']") # remove Ceann Comhairle 

               
               )


# Fixing data 
mask_2020_GE = ((df_electionsireland.year == 2020.0) &  # Include that year
                (~df_electionsireland['election_type'].isin(['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL'])) & # Exclude Election types
                (df_electionsireland.candidate_ID != "3694") & # Exclude speaker 
                (~df_electionsireland['status'].isin(['Appointed','awaiting update','Resigned']))  # Exclude status types

                )

mask_2016_GE = ((df_electionsireland.year == 2016.0) &  # Include that year
                (~df_electionsireland['election_type'].isin(['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL'])) & # Exclude Election types
                #(df_electionsireland.candidate_ID != "3694") & # Exclude speaker 
                (~df_electionsireland['status'].isin(['Appointed','awaiting update','Resigned']))  # Exclude status types

                )

mask_2011_GE = ((df_electionsireland.year == 2011.0) &  # Include that year
                (~df_electionsireland['election_type'].isin(['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL'])) & # Exclude Election types
                #(df_electionsireland.candidate_ID != "3694") & # Exclude speaker 
                (~df_electionsireland['status'].isin(['Appointed','awaiting update','Resigned']))  # Exclude status types

                )

mask_1982_GE = ((df_electionsireland.year == 1982.0) &  # Include that year
                (~df_electionsireland['election_type'].isin(['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL'])) & # Exclude Election types
                #(df_electionsireland.candidate_ID != "3694") & # Exclude speaker 
                (~df_electionsireland['status'].isin(['Appointed','awaiting update','Resigned']))  # Exclude status types

                )


### Masks ##

mask_presidential_election = (
                (~df_electionsireland['election_type'].isin(['SEANAD','BI-ELECTION', 'EUROPEAN', 'LOCAL','WESTMINSTER'])) & # Exclude Election types
                (df_electionsireland.constituency_name == "Ireland") & # Exclude speaker 
                (~df_electionsireland['status'].isin(['Appointed','awaiting update','Resigned']))  # Exclude status types

                )

df_electionsireland.loc[mask_2020_GE, 'election_type'] = 'GENERAL'
df_electionsireland.loc[mask_2016_GE, 'election_type'] = 'GENERAL'
df_electionsireland.loc[mask_2011_GE, 'election_type'] = 'GENERAL'
df_electionsireland.loc[mask_presidential_election, 'election_type'] = 'PRESIDENTIAL'




values_2020_fix = (df_electionsireland[df_electionsireland.year.isin([2020.0])]
               .query("election_type == ['GENERAL']") # remove seanad election
               
               )

values_2016_fix = (df_electionsireland[df_electionsireland.year.isin([2016.0])]
               .query("election_type == ['GENERAL']") # remove seanad election
               
               )

values_2011_fix = (df_electionsireland[df_electionsireland.year.isin([2011.0])]
               .query("election_type == ['GENERAL']") # remove seanad election
               
               )


### Upload ###
 # used in 
project_id= "red-bus-371614"        
credentials = service_account.Credentials.from_service_account_file('./../secrets/big_key.json')

destination_table = 'election_results.electionsireland'
what_to_if_table_exists = 'replace' 

upload(df_electionsireland,destination_table,'replace',project_id,credentials)# uploades dataframe to bigquery



### Modeling ### 
"""
    - Logistic regression : probability of being elected based on % of quota 
    - Use polling data to predict the votes in elections 
            - Use then to use current polls to feed into forcast 
"""
values_2011['status'] = values_2011['status'].map({'Elected': 1, 'Not Elected': 0})
values_2016['status'] = values_2016['status'].map({'Elected': 1, 'Not Elected': 0})
values_2020['status'] = values_2020['status'].map({'Elected': 1, 'Not Elected': 0})
values_all['status'] = values_all['status'].map({'Elected': 1, 'Not Elected': 0})
#LOCAL,EUROPEAN


sns.regplot(x=values_2011.pct_of_quota_reached_with_first_pref, y=values_2011.status, logistic=True, ci=None,label = "2011")
sns.regplot(x=values_2016.pct_of_quota_reached_with_first_pref, y=values_2016.status, logistic=True, ci=None,label = "2016")
sns.regplot(x=values_2020.pct_of_quota_reached_with_first_pref, y=values_2020.status, logistic=True, ci=None,label = "2020")
sns.regplot(x=values_all.pct_of_quota_reached_with_first_pref, y=values_all.status, logistic=True, ci=None,label = "2007")

plt.legend()
plt.show()

sns.regplot(x=values_all.pct_of_quota_reached_with_first_pref, y=values_all.status, logistic=True, ci=None,label = "all")
plt.legend()
plt.show()