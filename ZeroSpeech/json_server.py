#!/usr/bin/env python
# coding: utf-8

# In[90]:




import json

from pathlib import Path

import json

import wave
import contextlib
import os
import re
import math



# Speaker.json 바꾸기
def speaker_json(user_audio_path, org_audio_path):
    path_dir = str(user_audio_path)#,'./english/test/'] #경로 끝에 / 꼭 붙이기

    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬
    with open("./datasets/2019/english/speakers.json", "r") as st_json:
        speakers = json.load(st_json)
        for i in file_list:
            a=i.index('_')
            speaker=i[:a]
            if speaker not in speakers:
                speakers.append(speaker)
    with open("./datasets/2019/english/speakers.json", 'w', encoding='utf-8') as make_file:

        json.dump(speakers, make_file, indent="\t")

        # Speaker.json 바꾸기
    path_dir =str(org_audio_path)#경로 끝에 / 꼭 붙이기

    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬
    with open("./datasets/2019/english/speakers.json", "r") as st_json:
        speakers = json.load(st_json)
        for i in file_list:
            a=i.index('_')
            speaker=i[:a]
            if speaker not in speakers:
                speakers.append(speaker)
    with open("./datasets/2019/english/speakers.json", 'w', encoding='utf-8') as make_file:

        json.dump(speakers, make_file, indent="\t")

    

def train_json(user_audio_path):
    path_dir = str(user_audio_path)#경로 끝에 / 꼭 붙이기
    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬

    with open("./datasets/2019/english/train.json", "r") as st_json:
        train = json.load(st_json)
        for i in file_list:
            with contextlib.closing(wave.open(path_dir + i,'r')) as f:
                frames = f.getnframes()
                rate = f.getframerate()
                duration = frames / float(rate)
                duration=math.floor(duration*100)
                duration=duration/100
                fileplace=path_dir[2:]+i[0:-4]
                filepreprocessplace=i.index('_')
                a=[str(fileplace),0.0,duration,path_dir[2:16]+i[:filepreprocessplace]+'/'+i[0:-4]]
                if a not in train:
                    train.append(a)

    with open("./datasets/2019/english/train.json", 'w', encoding='utf-8') as make_file:

        json.dump(train, make_file, indent="\t")



def test_json(org_audio_path):
    path_dir =  str(org_audio_path)#경로 끝에 / 꼭 붙이기
    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort()
    with open("./datasets/2019/english/test.json", "r") as st_json:
        test = json.load(st_json)
        for i in file_list:
            with contextlib.closing(wave.open(path_dir + i,'r')) as f:
                frames = f.getnframes()
                rate = f.getframerate()
                duration = frames / float(rate)
                duration=math.floor(duration*100)
                duration=duration/100
                fileplace=path_dir[2:]+i[0:-4]
                filepreprocessplace=i.index('_')
                a=[str(fileplace),0.0,duration,path_dir[2:16]+i[:filepreprocessplace]+'/'+i[0:-4]]
                if a not in test:
                    test.append(a)

    with open("./datasets/2019/english/test.json", 'w', encoding='utf-8') as make_file:

        json.dump(test, make_file, indent="\t")


def synthesis_json(user_name, org_audio_path):
    user_name=str(user_name)
    path_dir=str(org_audio_path)
    file_list=os.listdir(path_dir)
    with open("./datasets/2019/english/synthesis2.json", "r") as st_json:
        synthesis = json.load(st_json)
        synthesis=[]
        for i in file_list:
            fileplace=path_dir[2:]+i[0:-4]
            plus=re.split(r'/', i)
            plus=plus[-1][:-4]
            save_name=user_name+'_'+plus
            a=[str(fileplace),user_name,save_name]
            if a not in synthesis:
                    synthesis.append(a)

    with open("./datasets/2019/english/synthesis2.json", 'w', encoding='utf-8') as make_file:

        json.dump(synthesis, make_file, indent="\t")

    
    


# In[ ]:





# In[ ]:




