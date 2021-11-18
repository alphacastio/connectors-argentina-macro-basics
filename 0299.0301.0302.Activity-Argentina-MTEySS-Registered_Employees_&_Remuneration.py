#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
from datetime import datetime
import requests

import re
from urllib.request import urlopen
from lxml import etree
import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[17]:


url = "https://www.trabajo.gob.ar/estadisticas/index.asp"
response = requests.get(url,verify=False)
html = response.content
htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
xls_address = tree.xpath("//*[@id='block-system-main']/section[2]/article/div/div[2]/div[2]/a/@href")[0]
xls_address


# In[18]:


url = xls_address
sheet = ['T.1','T.2.1','T.3.1','A.1','A.2.1','A.3.1','A.4','A.5.1']
r = requests.get(url,allow_redirects=False)


# In[19]:


#DF1: Trabajadores registrados. Total país.
df = pd.read_excel(r.content,skiprows=1,sheet_name=sheet[0])

df = df.dropna(how='all').dropna(how='all',axis=1)
df = df.dropna(how='all', subset = df.columns[1:]).dropna(subset=['Período'])
df = df.drop(['Unnamed: 2','Unnamed: 4'], axis=1)
df.rename(columns={'Período':'Date','Cantidad de Trabajadores (miles)':'Cantidad de Trabajadores','Nivel\nIndice base 100 = Ene-12':'Índice'}, inplace=True)

def fix_date(df):
    df["DateOk"] = pd.to_datetime(df["Date"], errors="coerce")
    df_to_fix = df[df["DateOk"].isnull()]
    df_to_fix["Date"] = df_to_fix["Date"].str.replace("*", "")

    df_to_fix["year"] = "20" + df_to_fix["Date"].str.split("-").str[1]
    df_to_fix["month"] = df_to_fix["Date"].str.split("-").str[0]
    df_to_fix["day"] = 1
    df_to_fix["month"] = df_to_fix["month"].str.lower()
    df_to_fix["month"] = df_to_fix["month"].replace(
        {
            "ene": "01",
            "feb": "02",
            "mar": "03",
            "abr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "ago": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dic": "12",

        })
    df_to_fix["DateOk"] = pd.to_datetime(df_to_fix[["year", "month", "day"]])
    df_fix = df[df["DateOk"].notnull()].append(df_to_fix)
    return df_fix

df = fix_date(df)
df = df.reset_index(drop=True)
df = df.drop(['year','month','day','Date'], axis=1)
df = df.rename(columns={'DateOk':'Date'})
df = df.set_index('Date')

newCols=[]
for col in df.columns:
    newCols += [col+' - '+'Serie original'+' - '+'Total país']        
df.columns = newCols

#DF2:Trabajadores registrados según modalidad ocupacional principal. Total país
df1 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[1])

df1 = df1.dropna(how='all').dropna(how='all',axis=1)
df1 = df1.dropna(how='all', subset = df1.columns[1:])
df1 = df1.rename(columns={'Período':'Date'}).replace('\n','')

df1 = fix_date(df1)
df1 = df1.drop(['year','month','day','Date'], axis=1)
df1 = df1.rename(columns={'DateOk':'Date'})
df1 = df1.set_index('Date')

for col in df1.columns:
    new_col = col.replace('\n',' ')
    df1 = df1.rename(columns={col: new_col})

newCols=[]
for col in df1.columns:
    new_Cols = re.sub(' +', ' ', col).replace("\n", "").replace
    newCols += [col+' - '+'Modalidad ocupacional principal']        
df1.columns = newCols

#DF3:Trabajadores registrados según modalidad ocupacional principal. Indice. Total país
df2 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[2])

df2 = df2.dropna(how='all').dropna(how='all',axis=1)
df2 = df2.dropna(how='all', subset = df2.columns[1:])
df2 = df2.rename(columns={'Período':'Date'}).replace('\n','')

df2 = fix_date(df2)

df2 = df2.drop(['year','month','day','Date'], axis=1)
df2 = df2.rename(columns={'DateOk':'Date'})
df2 = df2.set_index('Date')

for col in df2.columns:
    new_col = col.replace('\n',' ')
    df2 = df2.rename(columns={col: new_col})

newCols=[]
for col in df2.columns:
    newCols += [col+' - '+'Modalidad ocupacional principal - Índice']        
df2.columns = newCols

#DF4:Asalariados registrados del sector privado. Total país
df3 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[3])

df3 = df3.dropna(how='all').dropna(how='all',axis=1)
df3 = df3.dropna(how='all', subset = df3.columns[1:]).dropna(subset=['Período'])
df3 = df3.reset_index(drop=True)
df3 = df3.drop(['Unnamed: 2','Unnamed: 4'], axis=1)
df3.rename(columns={'Período':'Date','Cantidad de Trabajadores (en miles)':'Cantidad de Trabajadores','Nivel\nIndice base 100 = Ene-09':'Índice'}, inplace=True)

df3 = fix_date(df3)

df3 = df3.drop(['year','month','day','Date'], axis=1)
df3 = df3.rename(columns={'DateOk':'Date'})
df3 = df3.set_index('Date')

newCols=[]
for col in df3.columns:
    newCols += [col+' - '+'Serie original -  Sector privado']        
df3.columns = newCols

#DF5:Asalariados registrados del sector privado, según rama de actividad de la ocupación principal. Total país
df4 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[4])

df4 = df4.dropna(how='all').dropna(how='all',axis=1)
df4 = df4.dropna(how='all', subset = df4.columns[1:])
df4 = df4.rename(columns={'Período':'Date'})

df4 = fix_date(df4)

df4 = df4.drop(['year','month','day','Date'], axis=1)
df4 = df4.rename(columns={'DateOk':'Date'})
df4 = df4.set_index('Date')

for col in df4.columns:
    new_col = col.replace('\n',' ').replace('  ',' ')
    df4 = df4.rename(columns={col: new_col})

newCols=[]
for col in df4.columns:
    newCols += [col+' - '+'Sector privado - Actividad principal']        
df4.columns = newCols

#DF6:Asalariados registrados del sector privado, según rama de actividad de la ocupación principal. Indice. Total país
df5 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[5])

df5 = df5.dropna(how='all').dropna(how='all',axis=1)
df5 = df5.dropna(how='all', subset = df5.columns[1:])
df5 = df5.rename(columns={'Período':'Date'})

df5 = fix_date(df5)

df5 = df5.drop(['year','month','day','Date'], axis=1)
df5 = df5.rename(columns={'DateOk':'Date'})
df5 = df5.set_index('Date')

newCols=[]
for col in df5.columns:
    newCols += [col+' - '+'Sector privado - Actividad principal - Índice']        
df5.columns = newCols

dfFinal1 = df.merge(df1, how='outer', left_index=True, right_index=True).merge(df2, how='outer', left_index=True, right_index=True).merge(df3, how='outer', left_index=True, right_index=True).merge(df4, how='outer', left_index=True, right_index=True).merge(df5, how='outer', left_index=True, right_index=True)

dfFinal1["country"] = "Argentina"

alphacast.datasets.dataset(299).upload_data_from_df(dfFinal1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[20]:


#DF7:Remuneración de los asalariados registrados del sector privado. Total país
df6 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[6])

df6 = df6.dropna(how='all').dropna(how='all',axis=1)
df6 = df6.dropna(how='all', subset = df6.columns[1:])
df6.rename(columns={'Período':'Date','Nivel\nIndice base 100 = Ene-09':'Índice - Remuneración mediana','Nivel\nIndice base 100 = Ene-09.1':'Índice - Remuneración promedio'}, inplace=True)

df6 = fix_date(df6)

df6 = df6.drop(['year','month','day','Date'], axis=1)
df6 = df6.rename(columns={'DateOk':'Date'})
df6 = df6.set_index('Date')

newCols=[]
for col in df6.columns:
    newCols += [col+' - '+' Sector privado']        
df6.columns = newCols

df6["country"] = "Argentina"

alphacast.datasets.dataset(301).upload_data_from_df(df6, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


#DF8:Asalariados registrados del sector privado según provincia

df7 = pd.read_excel(r.content, skiprows = 1, sheet_name=sheet[7])

df7 = df7.dropna(how='all').dropna(how='all',axis=1)
df7 = df7.dropna(how='all', subset = df7.columns[1:])
df7 = df7.rename(columns={'Período':'Date'})

df7 = fix_date(df7)

df7 = df7.drop(['year','month','day','Date'], axis=1)
df7 = df7.rename(columns={'DateOk':'Date'})
df7 = df7.set_index('Date')

for col in df7.columns:
    new_col = col.replace('\n',' ')
    df7 = df7.rename(columns={col: new_col})

newCols=[]
for col in df7.columns:
    newCols += [col+' - '+' Asalariados sector privado']        
df7.columns = newCols

df7["country"] = "Argentina"

alphacast.datasets.dataset(302).upload_data_from_df(df7, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




