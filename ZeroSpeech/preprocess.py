import hydra
from hydra import utils
from pathlib import Path
import librosa
import scipy
import json
import numpy as np
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm


def preemphasis(x, preemph):
    return scipy.signal.lfilter([1, -preemph], [1], x)


def mulaw_encode(x, mu):
    mu = mu - 1
    fx = np.sign(x) * np.log1p(mu * np.abs(x)) / np.log1p(mu)
    return np.floor((fx + 1) / 2 * mu + 0.5)


def mulaw_decode(y, mu):
    mu = mu - 1
    x = np.sign(y) / mu * ((1 + mu) ** np.abs(y) - 1)
    return x


def process_wav(wav_path, out_path, sr=160000, preemph=0.97, n_fft=2048, n_mels=80, hop_length=160,
                win_length=400, fmin=50, top_db=80, bits=8, offset=0.0, duration=None):
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
            for in_path, start, duration, out_path in metadata: # json 파일 열어보면 순서대로 in_path, start, duration, out_path 정보를 주고 있음, 그러니 우리가 커스터마이징할 때 이 json 파일도 변경해야함
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


if __name__ == "__main__":
    preprocess_dataset()
