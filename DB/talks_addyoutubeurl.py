#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector


# In[2]:


data = pd.read_csv('../ted_scrap/data/data_mokloc.csv',encoding='latin-1')


# In[3]:


data2=pd.read_csv('../ted_scrap/data/ted_talks.csv',encoding='latin-1')


# In[4]:


data2['youtube_url']=None


# In[5]:


for i in range(len(data)):
    for j in range(len(data2)):
        if data['talk_id'][i]==data2['talk_id'][j]:
            data2['youtube_url'][j]=data['ted_url'][i]


# In[6]:


mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    port = 3306, ## 사용자가 지정한 port number (default : 3306)
    password = "1234", # 사용자가 지정한 password
    database = "tedbear", # 사용자가 지정한 db 이름
    auth_plugin='mysql_native_password')
mycursor = mydb.cursor()


# In[7]:


sql='Delete from talks'
mycursor.execute(sql)
mydb.commit()


# In[8]:


val1 = data2[['talk_id','url','youtube_url','title','speaker_1','published_date','duration',
          'topics','description','level','image']]
val1.rename(columns = {'talk_id' : 'id','speaker_1':'speaker'}, inplace = True)
val1 = val1.where(pd.notnull(val1), None)


# In[9]:


cols = "`,`".join([str(i) for i in val1.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in val1.iterrows():
    sql = "INSERT INTO talks (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    mycursor.execute(sql, tuple(row))
#    print(i)

    # the connection is not autocommitted by default, so we must commit to save our changes
mydb.commit()


# In[10]:


mycursor.close()
mydb.close()


# In[ ]:




