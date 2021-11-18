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

# In[2]:


url = 'https://www.utdt.edu/ver_contenido.php?id_contenido=8512&id_item_menu=16455'

pagina_utdt = requests.get(url).text


# In[3]:


soup = BeautifulSoup(pagina_utdt, 'html.parser')


# In[4]:


links = soup.find_all('a', href=True)


# In[5]:


for link in links:
    if 'Serie Histórica' in link:
        serie = 'https://www.utdt.edu' + link.get('href')


# In[6]:


#Se tiene que hacer un request para evadir el 403
xlsx_file = requests.get(serie)


# In[7]:


#Lee la respuesta
df = pd.read_excel(xlsx_file.content)
#Renombra la columna y le cambia el formato de fecha
df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
#Se renombran las variables
df.columns = ['Date', 'Median Inflation Expectations', 'Average Inflation Expectations']
df.set_index('Date', inplace=True)
df['country'] = 'Argentina'

alphacast.datasets.dataset(591).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

