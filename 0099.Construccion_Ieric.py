#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime
import numpy as np
import requests

from bs4 import BeautifulSoup
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = 'https://www.ieric.org.ar/series_estadisticas/series-estadisticas-nacionales/'


# In[3]:


page = requests.get(url).text
soup = BeautifulSoup(page, 'html.parser')


# In[4]:


#Extraigo los links
links = soup.find_all("a", href=True)


# In[5]:


#Actualizaron correctamente el link de puestos de trabajo
texto_puestos = "Puestos de trabajo registrados - Construcción"
texto_afcp ="Consumo de Cemento Portland (AFCP)"

links_list=[]

for link in links:
    if texto_puestos in link.text or texto_afcp in link.text:
        links_list.append(link.get('href'))


# In[6]:


#Consumo de cemento
df1 = pd.read_excel(links_list[1], sheet_name = 0, skiprows=4)
#Reduzco la cantidad de columnas
df1 = df1.iloc[:, :4]
#Cambio el formato de fecha y elimino las filas con NA
df1[df1.columns[0]] = pd.to_datetime(df1[df1.columns[0]], errors = 'coerce')
df1.dropna(axis=0, how='all', inplace=True)
df1.rename(columns={df1.columns[0]: 'Date'}, inplace=True)


# In[7]:


#Cantidad de puestos de trabajo
df2 = pd.read_excel(links_list[0], sheet_name = 0, skiprows=3)
#Conservo solo las primeras 2 columnas
df2 = df2.iloc[:, :2]
#Convierto a formato fecha y luego elimino las filas con NaN
df2[df2.columns[0]] = pd.to_datetime(df2[df2.columns[0]], errors = 'coerce')
df2.dropna(axis=0, how='all', inplace=True)
df2[df2.columns[1]] = df2[df2.columns[1]].astype(int)
df2.rename(columns={df2.columns[0]: 'Date'}, inplace=True)


# In[8]:


df= df1.merge(df2, on='Date', how='outer')


# In[9]:


df.set_index('Date', inplace=True)
df['country'] = 'Argentina'

alphacast.datasets.dataset(99).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:

