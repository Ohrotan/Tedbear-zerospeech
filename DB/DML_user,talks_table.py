#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector


# In[2]:


data = pd.read_csv('../ted_scrap/data/ted_talks.csv')
data = pd.DataFrame(data)


# In[4]:


# mysql workbench에서 지정한 db 정보
mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    port = 3306, ## 사용자가 지정한 port number (default : 3306)
    password = "1234", # 사용자가 지정한 password
    database = "tedbear" # 사용자가 지정한 db 이름
)
mycursor = mydb.cursor()

## user insert ## 
sql = "INSERT INTO user (id, email,name,pwd) VALUES (%s, %s, %s, %s)" 

## 임의값 지정
val = [
    (11,'fkfk@dassk.com','YangCHOI', 'ulsan'),
    (52,'lkdds@daksad.com','BAEK', 'busan'),
    (33,'alsd@kaskd.com','PARK', 'seoul'),
    (22,'dasd@dkasdk.com','HAN', 'jinju')
]
mycursor.executemany(sql, val)

mydb.commit()

print(mycursor.rowcount, "record was inserted")


# In[5]:


mycursor.close()
mydb.close()


# In[6]:


mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    port = 3306, ## 사용자가 지정한 port number (default : 3306)
    password = "1234", # 사용자가 지정한 password
    database = "tedbear" # 사용자가 지정한 db 이름
)


# In[7]:


mycursor = mydb.cursor()


# In[8]:


val1 = data[['talk_id','url','title','speaker_1','published_date','duration',
          'topics','description','level','image']]
val1.rename(columns = {'talk_id' : 'id','speaker_1':'speaker'}, inplace = True)
val1 = val1.where(pd.notnull(val1), None)


# In[9]:


cols = "`,`".join([str(i) for i in val1.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in val1.iterrows():
    sql = "INSERT INTO talks (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    mycursor.execute(sql, tuple(row))
    # the connection is not autocommitted by default, so we must commit to save our changes
mydb.commit()


# In[10]:


mycursor.close()
mydb.close()


# In[ ]:




