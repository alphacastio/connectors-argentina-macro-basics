#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


import ssl
ssl._create_default_https_context = ssl._create_unverified_context


# In[2]:


#La info proviene originalmente de https://www.economia.gob.ar/datos/, que carga un iframe de
#https://www.economia.gob.ar/series-tiempo-ar-landing/ y que carga por detras un json

df_links = pd.read_json('https://www.economia.gob.ar/series-tiempo-ar-landing/public/data/cards.json')


# In[3]:


#Descargo el json y filtro para obtener la url
xls_link = df_links[df_links['title'] == 'Empleo e Ingresos']['download_url'].values[0]
xls_file = requests.get(xls_link, verify=False).content


# In[4]:


#########
### RIPTE
#########
df1 = pd.read_excel(xls_file, sheet_name='RIPTE', engine='openpyxl', skiprows=9)
df1 = df1.iloc[:, :2]


# In[5]:


#Renombro la primera columna 
df1.rename(columns={df1.columns[0]: 'Date'}, inplace=True)

#Hago el reemplazo porque en algunos casos esta el mes tipeado y no como fecha
dict_meses={'ene': '01-01', 'feb': '02-01', 'mar':'03-01', 'abr':'04-01', 'may':'05-01', 'jun':'06-01', 'jul':'07-01',
           'ago':'08-01', 'sep':'09-01', 'oct':'10-01', 'nov':'11-01', 'dic':'12-01'}

df1['Date'].replace(dict_meses, regex=True, inplace=True)


# In[6]:

df1['Date'] = pd.to_datetime(df1['Date'], errors='coerce')
df1.dropna(subset=['Date'], inplace=True)

df1.set_index('Date', inplace=True)
df1['country'] = 'Argentina'


# In[7]:


######
#AUH
#####
df2 = pd.read_excel(xls_file, sheet_name='AUH', engine='openpyxl', skiprows=8)
df2 = df2.iloc[:, :2]


# In[8]:


#Renombro la primera columna 
df2.rename(columns={df2.columns[0]: 'Date'}, inplace=True)
df2['Date'] = pd.to_datetime(df2['Date'], errors='coerce')


df2.dropna(subset=['Date'], inplace=True)


# In[9]:


df2.set_index('Date', inplace=True)
df2['country'] = 'Argentina'


# In[10]:


###########
#Haber minimo jubilatorio
###########
df3 = pd.read_excel(xls_file, sheet_name='HaberMin', engine='openpyxl', skiprows=8)


# In[11]:


#mantengo solo las primeras 4 columnas
df3 = df3.iloc[:, :4]

#Hago un drop en base a la columna de moneda
df3.dropna(subset=['Moneda'], inplace=True)
df3.rename(columns={df3.columns[0]:'Date'}, inplace=True)


# In[12]:


#Cambio el formato de Date
df3['Date'] = pd.to_datetime(df3['Date'])

#Cambio la S por $ para que quede uniforme, el error de carga se da a partir de agosto 2020
df3['Moneda'] = df3['Moneda'].replace('S', '$')

df3.set_index('Date', inplace=True)
df3['country'] = 'Argentina'

alphacast.datasets.dataset(611).upload_data_from_df(df1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(612).upload_data_from_df(df2, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(613).upload_data_from_df(df3, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

