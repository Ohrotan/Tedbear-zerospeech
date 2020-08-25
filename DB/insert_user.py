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

print(mycursor.rowcount, "user record was inserted")


# In[5]:


mycursor.close()
mydb.close()



