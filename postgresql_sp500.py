#!/usr/bin/env python
# coding: utf-8

# # CREACIÓN DE LA BASE DE LA CONEXIÓN

# In[1]:


import psycopg2 as psy
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import create_engine
import sys


# In[2]:


# dbstring = 'Databasetype://usename:password@hostname:port/databasename'
my_database = 'acciones'
dbstring = f'postgresql://postgres:Plus7070@localhost:5432/{my_database}'
connection = create_engine(dbstring).connect()
cnx = create_engine(dbstring)
connection


# # CREACIÓN DE LA BASE DE DATOS

# In[3]:


pip install alpha_vantage


# In[4]:


from alpha_vantage.timeseries import TimeSeries


# In[5]:


API_key = 'S3DCI8BVT4ZHI0EI'
ts = TimeSeries(key = API_key, output_format='pandas')


# In[6]:


acciones_full = pd.read_excel('sp500_excell.xlsx')
acciones_full


# In[7]:


acciones_full['symbol'].loc[88]


# In[8]:


#si no hay datos nuevos vírgenes devolverá un error
bridge1 = pd.DataFrame() 

for i in range(len(acciones_full))[:4]:
  symbol = acciones_full['symbol'].loc[i]
  data = ts.get_weekly_adjusted(symbol)
  datos9 =data[0]
  datos9['symbol'] = acciones_full['symbol'].loc[i]
  datos9['companyName'] = acciones_full['companyName'].loc[i]
  bridge1 = bridge1.append([datos9])
  
bridge1


# In[ ]:


bridge2= pd.DataFrame(bridge1.reset_index())
bridge2


# # LECTURA DE LA BASE DE DATOS

# In[ ]:


# método para definir un query: 'myquery' de una tabla
my_table ='empresas'
my_query = f'''SELECT * FROM {my_table}'''


# In[ ]:


# método para leer un query de una database
df_query = pd.read_sql_query(my_query,con=cnx)
df_query


# In[ ]:


my_query = '''SELECT * FROM 'empresas' '''


# In[ ]:


table_name = 'empresas'
df = pd.read_sql_table(table_name, connection)
df


# # ESCRITURA Y QUERY A LA BASE DE DATOS

# In[ ]:


database='acciones'
engine =sqlalchemy.create_engine(f'postgresql://postgres:Plus7070@localhost:5432/{database}')
engine


# In[ ]:


#parámetros para if_exist son 'append'para añadir y 'replace' para machacar la db anterior
bridge2.to_sql('empresas', engine, if_exists='replace', index = False)


# In[ ]:


pd.read_sql('empresas',engine)


# In[ ]:


# método para definir un query: 'myquery' de una tabla
my_table ='empresas'
my_symbol = 'A'
my_query2 = f'''SELECT *FROM {my_table} WHERE symbol = '{my_symbol}' '''
print(my_query2)
df_query3 = pd.read_sql_query(my_query2,con=cnx)
df_query3


# In[ ]:


df_query3

