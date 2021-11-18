#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import io
from tqdm import tqdm


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url= "https://www.economia.gob.ar/download/infoeco/apendice6.xlsx"
r = requests.get(url, allow_redirects=False, verify=False)

skiprows= 3
sheetname = 'SPN'

with io.BytesIO(r.content) as datafr:
    df= pd.read_excel(datafr, skiprows=skiprows, sheet_name=sheetname, 
                      header=([0, 1, 2, 3]))


# In[3]:


df = df.dropna(how='all')
df = df.fillna('')

def replace_numbers(x):
    x = x.replace('(I) ', '')
    x = x.replace('(II) ', '')
    x = x.replace('(III) ', '')
    x = x.replace('(IV) ', '')
    x = x.replace('(V) ', '')
    return x

df = df.apply(lambda x: replace_numbers(x))

# transac_name = ''
# lista_col = []
# for value in df.columns:
#     if value[:7] != 'Unnamed':
#         transac_name = value
#         lista_col += [value]
#     else:
#         lista_col += [transac_name]

# df.columns = lista_col

# row_name = ''
# lista_row = []
# for value in df.iloc[0]:
#     if value[:7] != 'Unnamed':
#         row_name = value
#         lista_row += [value]
#     else:
#         lista_row += [row_name]

# df.iloc[0] = lista_row

# row_name1 = ''
# lista_row1 = []
# for i, value in enumerate(df.iloc[1]):
#     if (value[:7] == 'Unnamed') and (df.iloc[2][i] == 'Unnamed') and (df.iloc[3][i] == 'Unnamed') and (df.iloc[0][i] == 'Unnamed'):
#         lista_row1 += ['']
#     elif (value[:7] == 'Unnamed') and (df.iloc[2][i] == 'Unnamed') and (df.iloc[3][i] == 'Unnamed') and (df.iloc[0][i] != 'Unnamed'):
#         lista_row1 += ['']
#     elif (value[:7] != 'Unnamed') and (df.iloc[2][i] != 'Unnamed') and (df.iloc[3][i] != 'Unnamed'):
#         row_name1 = value
#         lista_row1 += [value]
#     elif (value[:7] == 'Unnamed') and (df.iloc[2][i] != 'Unnamed') and (df.iloc[3][i] != 'Unnamed'):
#         lista_row1 += [row_name1]
        
# df.iloc[1] = lista_row1


# In[4]:


# for i,value in enumerate(df.iloc[3]):
#     if (value == '') & (df.iloc[2][i] == '') & (df.iloc[1][i] == '') & (df.iloc[0][i] == ''):
#         df.iloc[3][i] = df.columns[i]
#     elif (value == '') & (df.iloc[2][i] == '') & (df.iloc[1][i] == ''):
#         df.iloc[3][i] = df.columns[i] + '-' + df.iloc[0][i]
#     elif (value == '') & (df.iloc[2][i] == ''):
#         df.iloc[3][i] = df.columns[i] + '-' + df.iloc[0][i] + '-' + df.iloc[1][i]
#     elif value == '':
#         df.iloc[3][i] == df.columns[i] + '-' + df.iloc[0][i] + '-' + df.iloc[1][i] + '-' + df.iloc[2][i]
#     else:
#         df.iloc[3][i] = str(df.columns[i]) + '-' + str(df.iloc[0][i]) + '-' + str(df.iloc[1][i]) + '-' + str(value)

# df.columns = df.iloc[3]
df.reset_index(drop=True, inplace=True)
# df


# In[5]:


column_0 = df.columns[0]
minrow = df[df[df.columns[0]]=='METODOLOGIA 1993 - 2006'].index[-1]


# In[6]:


minrow += 2
df = df[minrow:]
df.reset_index(drop=True, inplace=True)


# In[7]:


indexes_strings = []
for date in tqdm(df[df.columns[0]], desc='Looping over column names'):
    if 'METODOLOGIA' in str(date):
        indexes_strings.append(df[df[df.columns[0]] == str(date)].index[0])
        
indexes_values = []
for i in tqdm(range(0, len(indexes_strings)), desc='Looping over index values'):
    newval = indexes_strings[i] + 1
    indexes_values.append(newval)
    

df= df.drop(indexes_strings, axis=0)
df= df.drop(indexes_values, axis=0)

# new_columns=[]
# for column in df.columns:
#     column = column.replace('--','-')
#     column = column.replace('---','-')
#     new_columns += [column]

# df.columns = new_columns
    
df= df.rename(columns={column_0:('Date', 'Unnamed: 0_level_1', 'Unnamed: 0_level_2', 'Unnamed: 0_level_3')})


# In[8]:


df[('Date', '', '', '')] = df[('PERIODO', 'Unnamed: 0_level_1', 'Unnamed: 0_level_2', 'Unnamed: 0_level_3')]
df = df.set_index('Date')
df = df.drop(columns=[('PERIODO', 'Unnamed: 0_level_1', 'Unnamed: 0_level_2', 'Unnamed: 0_level_3')], axis=1)


# In[9]:


if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)

df['country'] = 'Argentina'

#Eliminamos todos los valores del indice que no son fecha
df.index = pd.to_datetime(df.index, errors='coerce')

df = df[df.index.notnull()]

alphacast.datasets.dataset(93).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
