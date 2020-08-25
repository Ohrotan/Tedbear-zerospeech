#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import mysql.connector
from ast import literal_eval # 문자형 딕셔너리 변환


# In[2]:


data = pd.read_csv('../ted_scrap/data/ted_talks.csv')
data = pd.DataFrame(data)
data = data.where(pd.notnull(data), None)


# In[3]:


a=list()
b=list()
for i in range(data.shape[0]):
    c=literal_eval(data['related_talks'][i])
    for j in range(0,len(c.keys())):
        a.append(list(c.keys())[j])
        b.append(list(c.values())[j])
d=list()
for j in range(0,len(data)):
    talk_id=data['talk_id'][j]
    c=literal_eval(data['related_talks'][j])
    for i in range(0,len(c)):
        d.append(talk_id)


# In[4]:


related=pd.DataFrame(columns=['talks_id','related_id','related_title'])
related['talks_id']=d
related['related_id']=a
related['related_title']=b


# In[6]:




mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    port = 3306,
    password = "1234",
    database = "tedbear"
)
mycursor = mydb.cursor()


# In[7]:


cols = "`,`".join([str(i) for i in related.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in related.iterrows():
    sql = "INSERT INTO related_talks (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    mycursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
mydb.commit()


# In[10]:


mycursor.close()
mydb.close()


# In[ ]:




