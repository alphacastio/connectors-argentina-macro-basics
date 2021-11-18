#!/usr/bin/env python
# coding: utf-8

# In[25]:


from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re
import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

links = []
for i in range(0,11):
    req = Request("https://www.enre.gov.ar/web/TARIFASD.nsf/todoscuadros?OpenView&Start={}".format(i*30+1))
    html_page = urlopen(req)

    soup = BeautifulSoup(html_page)

    for link in soup.findAll('a'):
        links.append(link.get('href'))


links = pd.DataFrame(links)[0] 
links = links[links.str.contains("opendocument")]


tabla = pd.read_html("https://www.enre.gov.ar/" + links.iloc[0])
df = tabla[0]
for i in range (1,9):
    df = df.append(tabla[i])
df.columns =  df.iloc[0]
df2 = df.pivot(columns="Tarifa 1 -R1", values=["EDENOR", "EDESUR"])
df2 = df2.fillna(method='ffill').iloc[2:]
df2.columns = [' - '.join(col).rstrip(' - ') for col in [c[::-1] for c in df2.columns.values]]
df2 = df2[df2.columns.drop(list(df2.filter(regex='Tarifa')))]
df2.columns = df2.columns.str.replace('Fijo', 'Fijo - $/mes -')
df2.columns = df2.columns.str.replace('Variable', 'Variable - $/kWh -')
df3 = df2

#for i in range(1, len(links)):

for i in range(1, 46):
    try:
        try:
            try:
                print(i)
                tabla = pd.read_html("https://www.enre.gov.ar/" + links.iloc[i])
                df = tabla[0]
                for f in range (1,9):
                    df = df.append(tabla[f])
                df.columns =  df.iloc[0]
                df2 = df.pivot(columns="Tarifa 1 -R1", values=["EDENOR", "EDESUR"])
                df2 = df2.fillna(method='ffill').iloc[2:]
                df2.columns = [' - '.join(col).rstrip(' - ') for col in [c[::-1] for c in df2.columns.values]]
                df2 = df2[df2.columns.drop(list(df2.filter(regex='Tarifa')))]
                df2.columns = df2.columns.str.replace('Fijo', 'Fijo - $/mes -')
                df2.columns = df2.columns.str.replace('Variable', 'Variable - $/kWh -')
                df3 = df3.append(df2)
            except KeyError:
                print("Nuevo método " + str(i))
                tabla = pd.read_html("https://www.enre.gov.ar/" + links.iloc[i])
                df5 = tabla[14][range(0,4)]
                for g in range(1,8):
                    df5 = df5.append(tabla[14+g][range(0,4)])
                df5.columns =  df5.iloc[1]
                df5 = df5.reset_index()
                df2 = df5.pivot(columns="Tarifa 1 -R1", values=["EDENOR", "EDESUR"])
                df2 = df2.fillna(method='ffill').iloc[2:]
                df2.columns = [' - '.join(col).rstrip(' - ') for col in [c[::-1] for c in df2.columns.values]]
                df2 = df2[df2.columns.drop(list(df2.filter(regex='Tarifa')))]
                df2.columns = df2.columns.str.replace('Fijo', 'Fijo - $/mes -')
                df2.columns = df2.columns.str.replace('Variable', 'Variable - $/kWh -')
                df2 = df2[df2.index==len(df2)+1]
                df3 = df3.append(df2)
        except KeyError:
            print("Nuevo método BIS " + str(i))
            tabla = pd.read_html("https://www.enre.gov.ar/" + links.iloc[i])
            df5 = tabla[0][range(0,4)]
            for g in range(1,8):
                df5 = df5.append(tabla[14+g][range(0,4)])
            df5.columns =  df5.iloc[1]
            df5 = df5.reset_index()
            df2 = df5.pivot(columns="Tarifa 1 -R1", values=["EDENOR", "EDESUR"])
            df2 = df2.fillna(method='ffill').iloc[2:]
            df2.columns = [' - '.join(col).rstrip(' - ') for col in [c[::-1] for c in df2.columns.values]]
            df2 = df2[df2.columns.drop(list(df2.filter(regex='Tarifa')))]
            df2.columns = df2.columns.str.replace('Fijo', 'Fijo - $/mes -')
            df2.columns = df2.columns.str.replace('Variable', 'Variable - $/kWh -')
            df2 = df2[df2.index==len(df2)+1]
            df3 = df3.append(df2)
    except:
        print(str(i) + ":'(")


# In[21]:


df3 = df3.reset_index()
df3 = df3.sort_index(ascending=False)
df3.index = pd.date_range(start='1/1/2018', periods=len(df3), freq = "MS")
df3.index.name = "Date"
del df3["index"]


df3["country"] = "Argentina"

alphacast.datasets.dataset(9999).upload_data_from_df(df3, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

