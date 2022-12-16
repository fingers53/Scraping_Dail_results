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
file_name_irelandelection = "C:/Users/fenlo/work repos/Scraping_Dail_results/irelandelection/ALL_CANDIDATES.parquet"
file_name_electionsireland = "C:/Users/fenlo/work repos/Scraping_Dail_results/electionsireland_data/ElectionsIreland_candidate.parquet"


df_irelandelection = pd.read_parquet(file_name_irelandelection)
df_electionsireland = pd.read_parquet(file_name_electionsireland)

