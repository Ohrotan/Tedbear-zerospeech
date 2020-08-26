#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import mysql.connector
from ast import literal_eval

def insert_user(val,port=3306,pwd=1234): 

#val에는     val = [('fkfk@dassk.com','YangCHOI', 'sha256'),('lkdds@daksad.com','BAEK', 'sha256')] 와 같이 
#[(email,name,pwd),(email,name,pwd)] 형식의 리스트가 와야함
#port의 default는 3306, 즉 로컬
#pwd에는 본인의 MySql 비밀번호. default는 1234

# mysql workbench에서 지정한 db 정보
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "tedbear",
        port = port, ## 사용자가 지정한 port number (default : 3306)
        password = str(pwd), # 사용자가 지정한 password
        database = "tedbear", # db
        auth_plugin='mysql_native_password')
    
    mycursor = mydb.cursor()
    ## user insert ## 
    sql = "INSERT INTO user ( email,name,pwd) VALUES ( %s, %s, %s)" 
    mycursor.executemany(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()




def init_talks(port=3306, pwd=1234):# talks 테이블을 초기 상태로 만드는 함수.
    data= pd.read_csv('../ted_scrap/data/data_mokloc.csv',encoding='latin-1')
    data2=pd.read_csv('../ted_scrap/data/ted_talks.csv',encoding='latin-1')
    data2['yt_url']=None
    for i in range(len(data)):
        for j in range(len(data2)):
            if data['talk_id'][i]==data2['talk_id'][j]:
                data2['yt_url'][j]=data['ted_url'][i][32:43] # youtube url에서 video id만 저장
                
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "tedbear",
        port = port, ## 사용자가 지정한 port number (default : 3306)
        password = str(pwd), # 사용자가 지정한 password
        database = "tedbear", # 사용자가 지정한 db 이름
        auth_plugin='mysql_native_password')
    
    mycursor = mydb.cursor()
    sql='Delete from talks'  # 초기화
    mycursor.execute(sql)
    mydb.commit()
    val1 = data2[['talk_id','url','yt_url','title','speaker_1','published_date','duration',
              'topics','description','level','image']]
    val1.rename(columns = {'talk_id' : 'id','speaker_1':'speaker'}, inplace = True)
    val1 = val1.where(pd.notnull(val1), None)
    cols = "`,`".join([str(i) for i in val1.columns.tolist()])
    for i,row in val1.iterrows():
        sql = "INSERT INTO talks (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        mycursor.execute(sql, tuple(row))
    mydb.commit()
    mycursor.close()
    mydb.close()
    

def insert_talks(val,port=3306,pwd=1234): # talks에 insert하는 함수. NULL값도 인자로 넣어야함

#val에는     val = [(id,url,yt_url,title,speaker,published_date,duration,topics,description,level,image),(id,url,yt_url,title,speaker,published_date,duration,topics,description,level,image)] 형식의 리스트가 와야함
#primary key인 id도 주는 이유는 id가 ted에서 제공하는 id이기때문..즉 함부로 autoincrement하면 안됨...
#port의 default는 3306, 즉 로컬
#pwd에는 본인의 MySql 비밀번호. default는 1234
    # mysql workbench에서 지정한 db 정보
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        port = port, ## 사용자가 지정한 port number (default : 3306)
        password = str(pwd), # 사용자가 지정한 password
        database = "tedbear", # db
        auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()
    ## talks insert ## 
    sql = "INSERT INTO talks (id,url,yt_url,title,speaker,published_date,duration,topics,description,level,image) VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)" 
    mycursor.executemany(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    
    


def init_related_talks(port=3306,pwd=1234): # related_talks를 초기 상태로 만드는 함수
    data = pd.read_csv('../ted_scrap/data/ted_talks.csv')
    data = pd.DataFrame(data)
    data = data.where(pd.notnull(data), None)
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
    related=pd.DataFrame(columns=['talks_id','related_id','related_title'])
    related['talks_id']=d
    related['related_id']=a
    related['related_title']=b
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        port = port,
        password = str(pwd),
        database = "tedbear",
        auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()
    sql='Delete from related_talks'  # 초기화
    mycursor.execute(sql)
    mydb.commit()
    cols = "`,`".join([str(i) for i in related.columns.tolist()])
    for i,row in related.iterrows():
        sql = "INSERT INTO related_talks (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        mycursor.execute(sql, tuple(row))
    mydb.commit()
    mycursor.close()
    mydb.close()


def insert_related_talks(val,port=3306,pwd=1234): # related_talks에 insert하는 함수. NULL값도 인자로 넣어야함

#val에는 val = [(talks_id,related_id,related_title),(talks_id,related_id,related_title)] 형식의 리스트가 와야함
#port의 default는 3306, 즉 로컬
#pwd에는 본인의 MySql 비밀번호. default는 1234

    # mysql workbench에서 지정한 db 정보
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        port = port, ## 사용자가 지정한 port number (default : 3306)
        password = str(pwd), # 사용자가 지정한 password
        database = "tedbear", # db
        auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()
    ## related_talks insert ## 
    sql = "INSERT INTO related_talks (talks_id,related_id,related_title) VALUES (%s, %s, %s, %s)" 
    mycursor.executemany(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()


def init_sentence(port=3306,pwd=1234): # sentence table을 초기화 하는 함수입니다.
    data = pd.read_csv('../ted_scrap/data/sentence_chunk_split.csv',encoding='latin-1')
    data = data.where(pd.notnull(data), None)

    data.rename(columns = {"start": "start_time",

     "end": "end_time"}, inplace = True)

    mydb = mysql.connector.connect(
        host = "localhost",
        user = "tedbear",
        port = port,
        password = str(pwd),
        database = "tedbear",
        auth_plugin='mysql_native_password')

    mycursor = mydb.cursor()
    sql='Delete from sentence'  # 초기화
    mycursor.execute(sql)
    mydb.commit()
    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    for i,row in data.iterrows():
        sql = "INSERT INTO sentence (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        mycursor.execute(sql, tuple(row))

    mydb.commit()
    mycursor.close()
    mydb.close()

    


def insert_sentence(val,port=3306,pwd=1234): # related_talks에 insert하는 함수. NULL값도 인자로 넣어야함

#val에는 val = [(talks_id,start_time,end_time,duration,sentence),(talks_id,start_time,end_time,duration,sentence)] 형식의 리스트가 와야함
#port의 default는 3306, 즉 로컬
#pwd에는 본인의 MySql 비밀번호. default는 1234

    # mysql workbench에서 지정한 db 정보
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        port = port, ## 사용자가 지정한 port number (default : 3306)
        password = str(pwd), # 사용자가 지정한 password
        database = "tedbear", # db
        auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()
    ## sentence insert ## 
    sql = "INSERT INTO sentence (talks_id,start_time,end_time,duration,sentence) VALUES (%s, %s, %s, %s, %s)" 
    mycursor.executemany(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()


