#!/usr/bin/env python
# coding: utf-8

# In[7]:


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

# In[8]:


url = "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/ajax/datos-operativos-exporta.php?tipo=xls&anio=%20%3E%201992&planilla=0302010-Cuadro_III_1"
url2= "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/ajax/datos-operativos-exporta.php?tipo=pdf&anio=%20%3E%201992&planilla=0302010-Cuadro_III_1"
headers = {
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "es-ES,es;q=0.9",
"Cache-Control": "no-cache",
"Connection": "keep-alive",
"Host": "www.ine.gub.uy",
"Pragma": "no-cache",
"Referer": "https://www.ine.gub.uy/web/guest/ipc-indice-de-precios-del-consumo",
"Sec-Fetch-Dest": "document",
"Sec-Fetch-Mode": "navigate",
"Sec-Fetch-Site": "same-origin",
"Sec-Fetch-User": "?1", "Host": "www.enargas.gob.ar",
"Connection": "keep-alive",
    "Accept-Encoding" : "gzip, deflate, br",
"Upgrade-Insecure-Requests": "1",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36",
"Cookie": "_ga=GA1.3.210024830.1628725458; _gid=GA1.3.1896292594.1628725458; PHPSESSID=k3l1ok6eocsru32dospl1b4i56; _gat=1"
}


# In[9]:


r = requests.get(url, verify = False)
df = pd.read_html(r.content)
df = df.pop(0)
df = df.iloc[:-2]
df = df[df.columns[~df.columns.str.contains(".1")]]
df.columns = ['Mes', 'Residencial', 'Comercial', 'Entes Oficiales', 'Industria',
       'Centrales Eléctricas', 'SDB', 'GNC', 'Total']
df.index = pd.date_range("1993-01-01", periods = len(df),freq="MS")
del df["Mes"]
df.index.rename('Date', inplace = True) 

df["country"] = "Argentina"

alphacast.datasets.dataset(610).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




