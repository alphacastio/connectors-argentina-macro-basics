#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


page = requests.get("https://www.grupoconstruya.com.ar/servicios/indice_construya", verify = False)


# In[ ]:


soup = BeautifulSoup(page.content, 'html.parser')


# In[ ]:


rows = soup.find_all('div', class_='Tabla')[0].find_all('div', class_='TablaRowIndice')


# In[ ]:


dataconestac = [[r.find('div', class_="conestac").text.replace("Con Estacionalidad: ", "")] for r in rows]
datadesestac = [[r.find('div', class_="desestac").text.replace("Desestacionalizado: ", "")] for r in rows]
mes = [r.find('div').text.replace('Mes:','') for r in rows]


# In[ ]:


df1=pd.DataFrame(mes[1:],columns=['Mes'])
df2=pd.DataFrame(datadesestac[1:],columns=['Desestacionalizado'])
df3=pd.DataFrame(dataconestac[1:],columns=['Con Estacionalidad'])
df=pd.concat([df1,df2,df3],axis=1)


# In[ ]:


m=df['Mes'].str.split()
df['MM']=m.str[0]
df['AA']=m.str[1]
M=df['MM'].str[0:3]
df['newmonth']=M.str.replace("Ago","Aug").str.replace('Abr','Apr').str.replace('Dic','Dec').str.replace('Ene','Jan')
df['newdate'] = '1'+' '+df['newmonth']+' '+df['AA']
def todate (element):
    d = datetime.strptime(element, '%d %b %Y')
    return d
df['Date']=df['newdate'].apply(todate)
cols=['Date','Con Estacionalidad','Desestacionalizado']
df=df[cols]


# In[ ]:


df = df.set_index("Date")
for column in df.columns:
    df[column] = pd.to_numeric(df[column].str.replace(",","."))


# In[ ]:


df.sort_index(inplace=True)
df["country"] = "Argentina"


alphacast.datasets.dataset(51).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




