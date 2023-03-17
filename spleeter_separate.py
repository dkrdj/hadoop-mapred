import os
import sys
import json
import subprocess

# spleeter 모듈 설치 경로 추가
sys.path.append('/path/to/spleeter')

# spleeter 모듈 임포트
from spleeter.separator import Separator

# 입력 인자로부터 입력 파일 경로를 가져옴
input_file = sys.argv[1]

# 입력 파일 경로에서 파일 이름과 확장자를 분리
file_name, ext = os.path.splitext(input_file)

# 보컬 분리 실행
separator = Separator('spleeter:2stems')
separator.separate_to_file(input_file, file_name)

# 분리된 보컬 파일 이름 리스트 생성
vocals_files = [f'{file_name}_vocals.wav', f'{file_name}_accompaniment.wav']

# 분리된 보컬 파일 이름 리스트를 json 형식으로 출력
print(json.dumps(vocals_files))