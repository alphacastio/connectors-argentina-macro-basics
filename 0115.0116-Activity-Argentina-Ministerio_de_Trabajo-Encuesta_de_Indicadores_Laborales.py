#!/usr/bin/env python
# coding: utf-8

# In[25]:




# import tabula
import pandas as pd
import requests
from urllib.request import urlopen
from lxml import etree
from collections import OrderedDict
from datetime import datetime
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[26]:





# In[27]:


data_url = "http://www.trabajo.gob.ar/estadisticas/eil/"
response = requests.get(data_url)

html = response.content

htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
file_urls = tree.xpath("//div[@class='row row-flex']/div[3]/a/@href")
file_url = "http://www.trabajo.gob.ar" + file_urls[0]
file_url


# In[28]:


print("reading file")
df = pd.read_excel(file_url, sheet_name="Total aglos 1.1", skiprows=3,header=[0,1])


# In[29]:


df_copy = df.copy()
n = 7
  
# Dropping last n rows using drop 
df_copy.drop(df_copy.tail(n).index, inplace = True) 


# In[30]:


df_replaced_with_empty = df_copy.replace(float('nan'), '', regex=True).replace('-', '', regex=True)


# In[31]:


df_Total_aglos_1_1 = df_replaced_with_empty.drop([df_replaced_with_empty.columns[2],df_replaced_with_empty.columns[3],df_replaced_with_empty.columns[4],df_replaced_with_empty.columns[5]], axis=1)
df_Total_aglos_1_1.columns = ['Date', 'Índice base ago. 01=100', 'Tasa de Entrada1', 'Tasa de Salida', 'Tasa de Rotación']


# In[32]:


df1 = pd.read_excel(file_url, sheet_name="Total aglos_1.2bis", skiprows=2,header=[0])


# In[33]:


df_copy = df1.copy()
df_copy.drop('Período', inplace=True, axis=1)


# In[34]:


df_copy.dropna(subset = ["Unnamed: 1"], inplace=True)
df_copy = df_copy.rename(columns={"Unnamed: 1": "Date"})


# In[35]:


df_remove_3_rows = df_copy.copy()
df_2 = df_remove_3_rows[3:16]


# In[36]:


df1_transposed = df_2.T # or df1.transpose()
new_columns_vals = df1_transposed.iloc[0]
df1_transposed.columns = new_columns_vals
df1_transposed = df1_transposed.iloc[1:]
# df1_transposed = df1_transposed.set_index('Date')


# In[37]:


import time
df1_transposed['Date'] = df1_transposed.index

def getdate_wo_hour(x):
    x= str(x)
    list_x = x.split(' ')
    x = list_x[0]
    return x

df1_transposed['Date'] = df1_transposed['Date'].apply(lambda x: getdate_wo_hour(x))
df1_transposed['Date'] = df1_transposed['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))


# In[38]:


df1_transposed = df1_transposed[df1_transposed.index.duplicated()==False]
df1_transposed = df1_transposed.loc[:,~df1_transposed.columns.duplicated()]

df_Total_aglos_1_1.index = pd.to_datetime(df_Total_aglos_1_1.index, format="%Y-%m-%d")


alphacast.datasets.dataset(115).upload_data_from_df(df_Total_aglos_1_1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[41]:


df1_transposed.index = pd.to_datetime(df1_transposed.index, format="%Y-%m-%d")
for col in df1_transposed.columns:
    df1_transposed[col] = pd.to_numeric(df1_transposed[col], errors="coerce")
    

alphacast.datasets.dataset(116).upload_data_from_df(df1_transposed, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
