#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import datetime
import numpy as np
import re
from datetime import datetime

import requests
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


lista_meses=['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']
for mes in lista_meses:
    url = f"https://www.redcame.org.ar/advf/documentos/2021/{mes}/60745517ac510.%20publicable.%20cantidades.xls"
    r = requests.get(url)
    if r.status_code == 200:
        df = pd.read_excel(url, sheet_name='Cuadro1', skiprows=3)

        def columnas(x):
            if type(x)==str: 
                x.replace('\n', ' ')
            else:
                pass
            return x

        newCols= []
        for col in df.columns:
            col = col.replace('\n', ' ').replace("(Var % i.a)",'').replace('%', 'porc.')
            newCols += [col]

        df.columns = newCols
        
        df = df.dropna(how='all', subset=df.columns[1:])

        df= df.rename(columns={'Unnamed: 1':'Mes', 'Periodo' : 'Año'})

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

        df['Año'] = df['Año'].astype(str)

        df['Date'] = df['Año'] + '-' + df['Mes']

        del df['Año']
        del df['Mes'] 

        df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

        df = df.set_index('Date')

        df['country'] = 'Argentina'
                       
        alphacast.datasets.dataset(279).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

        break
    else:
        print(f'We could not find a match on {mes}')


# In[12]:


df


# In[ ]:




