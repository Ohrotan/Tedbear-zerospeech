#!/usr/bin/env python
# coding: utf-8


from pathlib import Path
import wave
import contextlib
import os
import re
import math
import hydra
import hydra.utils as utils
import scipy
import torch
import numpy as np
import librosa
import pyloudnorm

from preprocess import preemphasis
from model import Encoder, Decoder
from pathlib import Path


import json

from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm

def preemphasis(x, preemph): # 고주파에서의 잡음을 줄이기 위한 전처리라고 합니다
    return scipy.signal.lfilter([1, -preemph], [1], x)


def mulaw_encode(x, mu): # wav파일이 encoder에 들어갈 형식
    mu = mu - 1
    fx = np.sign(x) * np.log1p(mu * np.abs(x)) / np.log1p(mu)
    return np.floor((fx + 1) / 2 * mu + 0.5)


def mulaw_decode(y, mu): # wav파일이 decoder에 들어갈 형식
    mu = mu - 1
    x = np.sign(y) / mu * ((1 + mu) ** np.abs(y) - 1)
    return x


def process_wav(wav_path, out_path, sr=160000, preemph=0.97, n_fft=2048, n_mels=80, hop_length=160,
                win_length=400, fmin=50, top_db=80, bits=8, offset=0.0, duration=None): # 오디오 전처리
    wav, _ = librosa.load(wav_path.with_suffix(".wav"), sr=sr,
                          offset=offset, duration=duration)
    wav = wav / np.abs(wav).max() * 0.999

    mel = librosa.feature.melspectrogram(preemphasis(wav, preemph),
                                         sr=sr,
                                         n_fft=n_fft,
                                         n_mels=n_mels,
                                         hop_length=hop_length,
                                         win_length=win_length,
                                         fmin=fmin,
                                         power=1)
    logmel = librosa.amplitude_to_db(mel, top_db=top_db)
    logmel = logmel / top_db + 1

    wav = mulaw_encode(wav, mu=2**bits)

    np.save(out_path.with_suffix(".wav.npy"), wav)
    np.save(out_path.with_suffix(".mel.npy"), logmel)
    return out_path, logmel.shape[-1]


@hydra.main(config_path="config/preprocessing.yaml") # 위의 confg_path의 파일의 모든 값이 함수의 인자로 들어오는 것
def preprocess_dataset(cfg):
    in_dir = Path(utils.to_absolute_path(cfg.in_dir)) # wav 파일 읽어올 디렉토리
    out_dir = Path(utils.to_absolute_path("datasets")) / str(cfg.dataset.dataset)  # 앞에 datasets가 붙어있으므로 경로는 datasets/{인자로 받은 값}이 됨
    out_dir.mkdir(parents=True, exist_ok=True)

    executor = ProcessPoolExecutor(max_workers=cpu_count())
    for split in ["train", "test"]: # "train"과 "test"를 차례로 반복
        print("Extracting features for {} set".format(split))
        futures = []
        split_path = out_dir / cfg.dataset.language / split # 위에서 정해진 out_dir 하위에 train과 test가 차례로 split_path가 됨, cfg.dataset.language는 config/preprocessing.yaml 안에 dataset: 2019/english라고 되어있는데 이 부분은 config/dataset/2019/english.yaml 파일로 연결됨, 이안에 language 변수 있음
        with open(split_path.with_suffix(".json")) as file: # train.json, test.json 차례로 불러오게됨
            metadata = json.load(file)
            for in_path, start, duration, out_path in metadata: # json 파일 열어보면 순서대로 in_path, start, duration, out_path 정보를 주고 있음
                wav_path = in_dir / in_path
                out_path = out_dir / out_path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                futures.append(executor.submit(
                    partial(process_wav, wav_path, out_path, **cfg.preprocessing,
                            offset=start, duration=duration)))

        results = [future.result() for future in tqdm(futures)]

        lengths = [x[-1] for x in results]
        frames = sum(lengths)
        frame_shift_ms = cfg.preprocessing.hop_length / cfg.preprocessing.sr
        hours = frames * frame_shift_ms / 3600
        print("Wrote {} utterances, {} frames ({:.2f} hours)".format(len(lengths), frames, hours))


# Speaker.json 바꾸기
def speaker_json(user_audio_path, org_audio_path):
    # 일단 train폴더 안의 speaker를 추가(웹 이용자가 추가될 것)
    path_dir = str(user_audio_path) #경로 끝에 / 꼭 붙이기 

    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬
    with open("./datasets/2019/english/speakers.json", "r") as st_json: #json 파일 읽기
        speakers = json.load(st_json)
        for i in file_list:
            a=i.index('_') #file_list 형식이 이름_번호 형식이라 잘라야함
            speaker=i[:a]
            if speaker not in speakers: #speakers.json에 없으면 추가
                speakers.append(speaker)
    with open("./datasets/2019/english/speakers.json", 'w', encoding='utf-8') as make_file:

        json.dump(speakers, make_file, indent="\t") # 추가성공

        # test 폴더안의 speaker 추가(ted 영상 speecher)
    path_dir =str(org_audio_path)#경로 끝에 / 꼭 붙이기

    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬
    with open("./datasets/2019/english/speakers.json", "r") as st_json:#json 파일 읽기
        speakers = json.load(st_json)
        for i in file_list:
            a=i.index('_')#file_list 형식이 이름_번호 형식이라 잘라야함
            speaker=i[:a]
            if speaker not in speakers:#speakers.json에 없으면 추가
                speakers.append(speaker)
    with open("./datasets/2019/english/speakers.json", 'w', encoding='utf-8') as make_file:

        json.dump(speakers, make_file, indent="\t") #추가성공

    

def train_json(user_audio_path): # 유저 정보를 기록하는 train.json 수정하기
    path_dir = str(user_audio_path)#경로 끝에 / 꼭 붙이기
    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort() #정렬

    with open("./datasets/2019/english/train.json", "r") as st_json:
        train = json.load(st_json)
        for i in file_list:
            with contextlib.closing(wave.open(path_dir + i,'r')) as f: # wav파일 읽기
                frames = f.getnframes()  #오디오 프레임의 수를 반환
                rate = f.getframerate()  #샘플링 빈도를 반환
                duration = frames / float(rate) #프레임/샘플링빈도 = 오디오의 길이
                duration=math.floor(duration*100) 
                duration=duration/100 #소숫점 처리 완료
                fileplace=path_dir[2:]+i[0:-4] #파일의 위치
                filepreprocessplace=i.index('_')
                a=[str(fileplace),0.0,duration,path_dir[2:16]+i[:filepreprocessplace]+'/'+i[0:-4]]
                #a=[파일의 위치, 오디오시작시간(0.0으로 통일), 오디오 길이, 전처리 시 저장될 위치]
                if a not in train:
                    train.append(a) # json에 없으면 추가

    with open("./datasets/2019/english/train.json", 'w', encoding='utf-8') as make_file:

        json.dump(train, make_file, indent="\t") #json에 추가 완료


def test_json(org_audio_path): # ted오디오의 정보를 기록하는 test.json 수정
    path_dir =  str(org_audio_path)#경로 끝에 / 꼭 붙이기
    file_list =os.listdir(path_dir) #경로 읽어 파일명 리스트 만들기
    file_list.sort()
    with open("./datasets/2019/english/test.json", "r") as st_json:
        test = json.load(st_json)
        for i in file_list:
            with contextlib.closing(wave.open(path_dir + i,'r')) as f: # wav파일 읽기
                frames = f.getnframes()#오디오 프레임의 수를 반환
                rate = f.getframerate() #샘플링 빈도를 반환
                duration = frames / float(rate)#프레임/샘플링빈도 = 오디오의 길이
                duration=math.floor(duration*100)
                duration=duration/100 #소숫점 처리 완료
                fileplace=path_dir[2:]+i[0:-4]#파일의 위치
                filepreprocessplace=i.index('_')
                a=[str(fileplace),0.0,duration,path_dir[2:16]+i[:filepreprocessplace]+'/'+i[0:-4]]
                #a=[파일의 위치, 오디오시작시간(0.0으로 통일), 오디오 길이, 전처리 시 저장될 위치]
                if a not in test:
                    test.append(a)

    with open("./datasets/2019/english/test.json", 'w', encoding='utf-8') as make_file:

        json.dump(test, make_file, indent="\t")  #json에 추가 완료


def synthesis_json(user_id, org_audio_path): # 음성 합성할 때 쓸 synthesis_list.json 변경
    user_name=str(user_id) #user_id로 speaker이용
    path_dir=str(org_audio_path) #변환 대상이 될 ted영상의 경로
    file_list=os.listdir(path_dir) 
    with open("./datasets/2019/english/synthesis_list.json", "r") as st_json: 
        synthesis = json.load(st_json)
        synthesis=[] #초기화
        for i in file_list:
            fileplace=path_dir[2:]+i[0:-4] #ted영상의 path
            plus=re.split(r'/', i) 
            plus=plus[-1][:-4] #필요한 정보만 슬라이스. 
            save_name=user_name+'_'+plus # 변환된 파일 이름
            a=[str(fileplace),user_name,save_name] 
            if a not in synthesis:
                    synthesis.append(a)

    with open("./datasets/2019/english/synthesis_list.json", 'w', encoding='utf-8') as make_file:

        json.dump(synthesis, make_file, indent="\t") # 추가 완료




@hydra.main(config_path="config/convert.yaml")
def convert(cfg): 
    '''
    yaml파일의 구조는 아래와 같다
    
    defaults:
    - dataset: 2019/english
    - preprocessing: default
    - model: default

synthesis_list: datasets/2019/english/synthesis_list.json
in_dir: ./
out_dir: submission/2019/english/result
checkpoint: checkpoints/2019english/model.ckpt-500000.pt

위에서 synthesis_list는 변환시킬 목록이다. 위에서 만들었던 synthesis_list.json이 들어온다
in_dir + synthesis_list의 ted영상 위치가 wav_path로 들어갈 것이다. 즉 ted영상의 위치
out_dir은 음성합성이 완료된 오디오파일의 저장 위치
checkpoint는 우리가 쓸 모델의 위치


'''
    
    dataset_path = Path(utils.to_absolute_path("datasets")) / cfg.dataset.path
    with open(dataset_path / "speakers.json") as file:
        speakers = sorted(json.load(file))

    synthesis_list_path = Path(utils.to_absolute_path(cfg.synthesis_list))
    with open(synthesis_list_path) as file:
        synthesis_list = json.load(file)

    in_dir = Path(utils.to_absolute_path(cfg.in_dir))
    out_dir = Path(utils.to_absolute_path(cfg.out_dir))
    out_dir.mkdir(exist_ok=True, parents=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    encoder = Encoder(**cfg.model.encoder)
    decoder = Decoder(**cfg.model.decoder)
    encoder.to(device)
    decoder.to(device)

    print("Load checkpoint from: {}:".format(cfg.checkpoint))
    checkpoint_path = utils.to_absolute_path(cfg.checkpoint)
    checkpoint = torch.load(checkpoint_path, map_location=lambda storage, loc: storage)
    encoder.load_state_dict(checkpoint["encoder"])
    decoder.load_state_dict(checkpoint["decoder"])

    encoder.eval()
    decoder.eval()

    meter = pyloudnorm.Meter(cfg.preprocessing.sr)

    for wav_path, speaker_id, out_filename in tqdm(synthesis_list):
        wav_path = in_dir / wav_path
        wav, _ = librosa.load(
            wav_path.with_suffix(".wav"),
            sr=cfg.preprocessing.sr)
        ref_loudness = meter.integrated_loudness(wav)
        wav = wav / np.abs(wav).max() * 0.999

        mel = librosa.feature.melspectrogram(
            preemphasis(wav, cfg.preprocessing.preemph),
            sr=cfg.preprocessing.sr,
            n_fft=cfg.preprocessing.n_fft,
            n_mels=cfg.preprocessing.n_mels,
            hop_length=cfg.preprocessing.hop_length,
            win_length=cfg.preprocessing.win_length,
            fmin=cfg.preprocessing.fmin,
            power=1)
        logmel = librosa.amplitude_to_db(mel, top_db=cfg.preprocessing.top_db)
        logmel = logmel / cfg.preprocessing.top_db + 1

        mel = torch.FloatTensor(logmel).unsqueeze(0).to(device)
        speaker = torch.LongTensor([speakers.index(speaker_id)]).to(device)
        with torch.no_grad():
            z, _ = encoder.encode(mel)
            output = decoder.generate(z, speaker)

        output_loudness = meter.integrated_loudness(output)
        output = pyloudnorm.normalize.loudness(output, output_loudness, ref_loudness)
        path = out_dir / out_filename
        librosa.output.write_wav(path.with_suffix(".wav"), output.astype(np.float32), sr=cfg.preprocessing.sr)

def get_converted_audio(user_id, user_audio_path, org_audio_path) : #위 함수들을 한번에 실행
    import hydra
    speaker_json(user_audio_path, org_audio_path)
    train_json(user_audio_path)
    test_json(org_audio_path)
    synthesis_json(user_id, org_audio_path)
    preprocess_dataset()
    hydra._internal.hydra.GlobalHydra().clear()
    convert()
    hydra._internal.hydra.GlobalHydra().clear()
