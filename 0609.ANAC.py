#!/usr/bin/env python
# coding: utf-8

# In[24]:


import pandas as pd
import numpy as np
import datetime
import urllib
import time
from urllib.request import urlopen
import requests  

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[31]:


url = "https://datos.anac.gob.ar/static/docs/serie-historica-anac-01-19.xlsx"
url2 = "https://www.anac.gob.ar/anac/web/uploads/estadisticas/series-hist-ricas-anac.xlsx"

df = pd.read_excel(url, skiprows= 9)
df = df.iloc[[1,2,3]]


# In[32]:


cols = [0,1,2,3,4,6,7]
df = df.drop(df.columns[cols],axis=1)
df = df.T
df.columns = df.iloc[0]
df = df.iloc[1: , :]
df.index = pd.date_range("2001-01-01", periods = len(df) ,freq="M")
df.index.rename('Date', inplace = True) 
df.columns.name = ""


# In[76]:


df2 = pd.read_excel(url2, skiprows= 6)
df2 = df2.iloc[[1,2,3]]
df2 = df2.T
df2.columns = df2.iloc[0]
df2 = df2.iloc[1: , :]
df2.index = pd.date_range("2019-01-01", periods = len(df2) ,freq="M")
df2.index.rename('Date', inplace = True) 
df2.columns.name = ""


# In[81]:


df_final = df.iloc[:-21].append(df2)
df_final.columns = ['Cabotaje', 'Internacional', 'Total']
df_final.columns = "Pasajeros aéreos - " + df_final.columns

df_final["country"] = "Argentina"

alphacast.datasets.dataset(609).upload_data_from_df(df_final, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




