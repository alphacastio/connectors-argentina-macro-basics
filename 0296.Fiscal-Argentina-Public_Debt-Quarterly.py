#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


url = "https://www.argentina.gob.ar/sites/default/files/deuda_publica_31-12-2020.xlsx"
r = requests.get(url, allow_redirects=False,verify=False)

df = pd.read_excel(r.content,skiprows = 9, sheet_name = 'A.4.5')

df = df.dropna(how = 'all').dropna(how='all', axis = 1)
df = df.dropna(how='all', subset=df.columns[1:])
df = df.rename(columns = {'Período': 'Date'})
df = df.set_index('Date')

df['country'] = 'Argentina'

alphacast.datasets.dataset(296).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



