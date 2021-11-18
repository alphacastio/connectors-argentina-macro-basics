#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
import datetime
import numpy as np
import re
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


url = "https://www.redcame.org.ar/documentos/5915"
r = requests.get(url,verify=False)
df = pd.read_excel(r.content, sheet_name='Cuadro1', skiprows=4)


df= df.rename(columns={'Unnamed: 1':'Mes', 'Unnamed: 0' : 'Año'})

df = df.loc[:, ~(df == 'Var.i.a').any()]

df = df.dropna(how='all', axis=0)
df = df.dropna(how='all', axis=1)

df['Año'] = df['Año'].fillna(method='ffill')

def replaceDate(x):
    if x=='Enero':
        x='01-01'
    elif x=='Febrero':
        x='02-01'
    elif x=='Marzo':
        x='03-01'
    elif x=='Abril':
        x='04-01'
    elif x=='Mayo':
        x='05-01'
    elif x=='Junio':
        x='06-01'
    elif x=='Julio':
        x='07-01'
    elif x=='Agosto':
        x='08-01'
    elif x=='Septiembre':
        x='09-01'
    elif x=='Octubre':
        x='10-01'
    elif x=='Noviembre':
        x='11-01'
    elif x=='Diciembre':
        x='12-01'
    return x
df['Mes'] = df['Mes'].apply(lambda x: replaceDate(x))

df['Año'] = df['Año'].astype(str).apply(lambda x: x.replace('.0', ''))

df['Date'] = df['Año'] + '-' + df['Mes']

del df['Año']
del df['Mes'] 

df = df.dropna(how='any', axis=0)

df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

df = df.set_index('Date')

df['country'] = 'Argentina'
del df["Unnamed: 25"]

alphacast.datasets.dataset(278).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




