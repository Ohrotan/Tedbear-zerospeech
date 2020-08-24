#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import mysql.connector


# In[2]:


data = pd.read_csv('../ted_scrap/data/final.csv')
data = data.where(pd.notnull(data), None)


# In[3]:


data.rename(columns = {"start": "start_time",

 "end": "end_time"}, inplace = True)


# In[4]:




mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    port = 3306,
    password = "1234",
    database = "tedbear",
     auth_plugin='mysql_native_password')

mycursor = mydb.cursor()


# In[5]:


cols = "`,`".join([str(i) for i in data.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in data.iterrows():
    sql = "INSERT INTO sentence (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    mycursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
mydb.commit()


# In[6]:


mycursor.close()
mydb.close()


# In[ ]:




