from datetime import datetime, timezone #지금 시간, utc 시간대
from pathlib import Path #파일/폴더 경로를 다루는 도구
import shutil # 다운로드 내용을 파일로 복사
import urllib.request #인터넷에서 파일 받기
import hashlib # sha256 지문 계산
import json #lineage 를 json 글자로 바꾸기
import os # os.rename 이름 바꾸기
import zipfile # zip 인지 아닌지 검사

from airflow.sdk import dag, task, get_current_context

BTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
LANDING_ROOT = Path("/opt/airflow/data/landing/")  


@dag(schedule=None, catchup=False)
def bts_landing():

    @task
    def download_bts(year: int = 2025, month: int = 1) -> str:
        ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")  

        month_dir = LANDING_ROOT / 'bts' / f"year={year}" / f"month={month:02d}"
        final_dir = month_dir / f"ingested_at={ingested_at}"
        # tmp folder path (will rename this to the final_dir)
        tmp_dir = month_dir / f"_tmp_ingested_at={ingested_at}"
        tmp_dir.mkdir(parents=True, exist_ok=False) #parents: 중간 폴더도 같이 만들기 / #exist_ok: 이미 있으면, 덮어쓰기
        
        # Download zip file
        url = BTS_URL.format(year=year, month=month) 
        zip_name = url.rsplit("/", 1)[-1]
        tmp_zip = tmp_dir / zip_name  

        with urllib.request.urlopen(url) as resp, open(tmp_zip, "wb") as f: # BTS 에 접속해서 response 를 받음 (404면 여기서 바로 에러) & 작업 공간에 파일을 쓰기(w) + 바이너리(b) 모드로 엶
            shutil.copyfileobj(resp,f) #응답을 조금씩 읽어서 파일에 씀

        # 파일 download 중도 fail 방지
        if not zipfile.is_zipfile(tmp_zip):
            raise ValueError(f"Downloaded file is not a valid zip: {url}")
        
        # lineage json file creation
        manifest = {
            "source_url": url, 
            "ingested_at": ingested_at, 
            "sha256": hashlib.sha256(tmp_zip.read_bytes()).hexdigest(), # 지문계산(바이트의 전부).64글자로
            "bytes": tmp_zip.stat().st_size, #파일정보().크기
            "dag_run_id": get_current_context()["run_id"],
            "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), # 지문계산(이 DAG 파일 자신의 경로).64글자로
        }
        (tmp_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2)) #작업공간에 json 저장

        # rename and return
        os.rename(tmp_dir, final_dir)
        return str(final_dir / zip_name)



    download_bts()


bts_landing()