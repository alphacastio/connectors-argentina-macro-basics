#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
from datetime import datetime
import requests

import re
from urllib.request import urlopen
from lxml import etree
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[8]:


url = "http://www.trabajo.gob.ar/estadisticas/oede/estadisticasnacionales.asp"
response = requests.get(url,verify=False)
html = response.content
htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
xls_address = tree.xpath("//*[@id='block-system-main']/section/article/div/div[9]/div/div[1]/div/div/ul[1]/li[1]/a/@href")[0]
xls_address


# In[9]:


url = 'http://www.trabajo.gob.ar' + xls_address
r = requests.get(url, allow_redirects=True, verify=False)


# In[10]:


worksheets = {
    'C 1': {"prefix": 'Promedio',
           "indice_final": 'Variaciones (%)'}, 
    'C 2': {"prefix": 'Mediana',
           "indice_final": 'Variaciones (%)'}, 
    'C 3': {"prefix": 'Trabajadores con 5 años de antigüedad o más',
           "indice_final": 'Variaciones (%)'},
    'C 4': {"prefix": 'Dispersion salarial',
           "indice_final": 'Diferencia en puntos porcentuales'}
}

df_merge_1 = pd.DataFrame()

for key in worksheets.keys():

    df = pd.read_excel(r.content, skiprows= 4, sheet_name=(key),header=[0,1,2])
    df = df.dropna(how='all').dropna(how='all',axis=1)
    df = df.iloc[:,0:7]

    if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.map(' - '.join)

    for col in df.columns:
        new_col = re.sub('\n', ' ', col).replace("Unnamed: 0_level_0 - Período - Unnamed: 0_level_2", "Date").replace("(*)","")
        df = df.rename(columns={col: new_col})

    indiceFinal = df[df['Date'] == worksheets[key]["indice_final"]].index[0]
    df = df[:indiceFinal-2]

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.set_index('Date')

    for col in df.columns:
        df = df.rename(columns={col: worksheets[key]["prefix"]+' - '+col}) 
        
    df_merge_1 = df_merge_1.merge(df, how='outer', left_index=True, right_index=True)

df_merge_1["country"] = "Argentina"
df_merge_1


# In[11]:


worksheets_2 = {
    'C 5.1': {"prefix": 'Remuneración promedio',
             "indice_final": 'Interanual'}, 
    'C 5.2': {"prefix": 'Remuneración promedio - Desestacionalizada',
             "indice_final": 'Último mes'}, 
    'C 5.3': {"prefix": 'Remuneración promedio - Tendencia ciclo',
             "indice_final": 'Último mes'},
    'C 6': {"prefix": 'Remuneración mediana',
           "indice_final": 'Interanual'}
}

colnames_Dict = {
    'A':'AGRICULTURA, GANADERIA, CAZA Y SILVICULTURA',
    'B':'PESCA',
    'C':'EXPLOTACION DE MINAS Y CANTERAS',
    'D': 'INDUSTRIAS MANUFACTURERAS',
    'E':'SUMINISTRO DE ELECTRICIDAD, GAS Y AGUA',
    'F':'CONSTRUCCION',
    'G':'COMERCIO Y REPARACIONES',
    'H':'HOTELES Y RESTAURANTES',
    'I':'TRANSPORTE, ALMACENAMIENTO Y COMUNICACIONES',
    'J':'INTERMEDIACION FINANCIERA',
    'K':'ACTIVIDADES INMOBILIARIAS, EMPRESARIALES Y DE ALQUILER',
    'M':'ENSEÑANZA',
    'N':'SERVICIOS SOCIALES Y DE SALUD',
    'O':'OTRAS ACTIVIDADES DE SERVICIOS COMUNITARIAS, SOCIALES Y PERSONALES'
}

df_merge_2 = pd.DataFrame()

for key in worksheets_2.keys():

    df = pd.read_excel(r.content, skiprows= 4, sheet_name=(key))
    df = df.dropna(how='all').dropna(how='all',axis=1)

    df = df.rename(columns=colnames_Dict)

    indiceFinal = df[df['Período'] == worksheets_2[key]["indice_final"]].index[0]
    df = df[:indiceFinal-4]

    df["Date"] = pd.to_datetime(df["Período"], errors="coerce")
    df = df.set_index('Date')
    del df['Período']
    
    for col in df.columns:
        df = df.rename(columns={col: worksheets_2[key]["prefix"]+' - '+col}) 
        
    df_merge_2 = df_merge_2.merge(df, how='outer', left_index=True, right_index=True)

df_merge_2["country"] = "Argentina"

alphacast.datasets.dataset(501).upload_data_from_df(df_merge_1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(502).upload_data_from_df(df_merge_2, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




