#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install lxml 
import pandas as pd
import requests, json
import numpy as np
# from urllib.request import urlopen
from lxml import etree
from collections import OrderedDict
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:





# In[3]:


data_url = "http://www.fiel.org/estadisticas"
response = requests.get(data_url)

html = response.content


# In[4]:


htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
file_urls = tree.xpath("//div[@class='col_ampliada']/div[3]/ul/li/a/@href")
file_url = "http://www.fiel.org" + file_urls[0]
file_url


# In[5]:


df = pd.read_excel(file_url, skiprows=2,header=[0,1,2])
df


# In[6]:


df_dropped_columns = df.drop([df.columns[2],df.columns[3],df.columns[5],df.columns[7],df.columns[8],df.columns[10],df.columns[11],df.columns[13],df.columns[14],df.columns[16],df.columns[17]] , axis=1)
df_dropped_columns


# In[7]:


df_dropped_columns.columns = ['Date', 'IPI(con Estacionalidad)', 'IPI(Desestacionalizado)', 'BCND', 'BCD', 'BUI', 'BK', 'ALIMENTOS Y BEBIDAS', 'CIGARRILLOS', 'INSUMOS TEXTILES', 'PASTA Y PAPEL', 'COMBUSTIBLE', 'QUIMICOS Y PLASTICOS', 'MINERALES NO METALICOS', 'SIDERURGIA', 'METALMECANICA', 'AUTOMÓVILES'] 
df_dropped_columns


# In[8]:


df_replaced_with_empty = df_dropped_columns.replace(float('nan'), '', regex=True)
df_replaced_with_empty


# In[9]:


year_value = '2013'
previous_month_val = ''
for i, item in enumerate(df_replaced_with_empty['Date']):
    temp_list = item.split(' ')
    if len(temp_list) > 1:
        temp_year = temp_list[1]
        try: 
            int(temp_year)
            year_value = temp_year
            df_replaced_with_empty['Date'][i] = year_value + '-01'
        except ValueError:
            pass
    
    if temp_list:
        temp_month_val = temp_list[0]
        real_month_val = ''
        if temp_month_val == 'F':
            real_month_val = '-02-01'
        elif temp_month_val == 'M' and previous_month_val == 'F':
            real_month_val = '-03-01'
        elif temp_month_val == 'A' and previous_month_val == 'M':
            real_month_val = '-04-01'
        elif temp_month_val == 'M' and previous_month_val == 'A':
            real_month_val = '-05-01'
        elif temp_month_val == 'J' and previous_month_val == 'M':
            real_month_val = '-06-01'
        elif temp_month_val == 'J' and previous_month_val == 'J':
            real_month_val = '-07-01'
        elif temp_month_val == 'JL':
            real_month_val = '-07-01'
        elif temp_month_val == 'A' and (previous_month_val == 'J' or previous_month_val == 'JL'):
            real_month_val = '-08-01'
        elif temp_month_val == 'S':
            real_month_val = '-09-01'
        elif temp_month_val == 'O':
            real_month_val = '-10-01'
        elif temp_month_val == 'N':
            real_month_val = '-11-01'
        elif temp_month_val == 'D':
            real_month_val = '-12-01'
            
        if real_month_val:
            df_replaced_with_empty['Date'][i] = year_value + real_month_val
            previous_month_val = temp_month_val        
        
df_replaced_with_empty


# In[10]:


index=df_replaced_with_empty[df_replaced_with_empty['Date'] == 'I Trim.13'].index[0]
index -= 1

df_replaced_with_empty = df_replaced_with_empty[:index]
df_replaced_with_empty['Date'] = pd.to_datetime(df_replaced_with_empty['Date'])
df_replaced_with_empty = df_replaced_with_empty.set_index('Date')
df_replaced_with_empty['country'] = 'Argentina'
df_replaced_with_empty


# In[11]:


for col in df_replaced_with_empty.columns[:-1]:
    df_replaced_with_empty[col] = df_replaced_with_empty[col].astype(float)

df_replaced_with_empty['country'] = df_replaced_with_empty['country'].astype(str)


alphacast.datasets.dataset(103).upload_data_from_df(df_replaced_with_empty, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



