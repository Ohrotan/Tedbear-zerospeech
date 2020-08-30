#!/usr/bin/env python
# coding: utf-8


import json

def speaker_changer(result,user_id):
    with open("./datasets/2019/english/speakers.json", "r") as st_json:
        speakers = json.load(st_json)

    for n, i in enumerate(speakers):
        
        if i == result:
            index=speakers.index(result)
            speakers[index] = str(user_id)
    with open("./speakers.json", 'w', encoding='utf-8') as make_file:
        json.dump(speakers, make_file, indent="\t")


# speaker_changer(result,user_name)
# speaker_changer('S020','kang')

