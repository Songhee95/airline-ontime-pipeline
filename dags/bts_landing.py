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
BRONZE_ROOT = Path("/opt/airflow/data/bronze/")


@dag(schedule=None, catchup=False)
def bts_landing():

    @task
    def download_bts(year: int = 2025, month: int = 1) -> dict:
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

        # 파일 download atomic (다운로드가 끝까지 성공 해야만 최종 폴더가 생김)
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
        
        return {
            "zip_path": str(final_dir / zip_name),
            'year': year,
            'month':month, 
            'ingested_at': ingested_at,
            'sha256': manifest['sha256']
        }

    @task
    def to_bronze(info: dict) -> str:
        import duckdb
        import tempfile
        # 입력 정보 꺼내기
        zip_path=Path(info['zip_path']) 

        # 출력 위치 정하기
        # p_year / p_month: duckdb가 자동으로 폴더 라벨 컬럼 생성. 이미 year 과 month 가 있는 컬럼이라서 risky.
        # 폴더 라벨 컬럼 타입은 duckdb 에 의해 추측된 타입.
        bronze_dir = BRONZE_ROOT / 'bts' / f"p_year={info['year']}" / f"p_month={info['month']:02d}" # bronze/bts/p_year=2025/p_month=01
        bronze_dir.mkdir(parents=True, exist_ok=True)
        final = bronze_dir / "data.parquet"
        tmp = bronze_dir / "data.parquet.tmp" # same folder as final

        # zip 안의 csv 확인 + 임시로 풀기 
        with zipfile.ZipFile(zip_path) as zf: # open zip file
            csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")] # get all the files in the zip folder & get only files with .csv.
            if len(csvs) != 1:
                raise ValueError(f"Expected exactly 1 CSV in {zip_path}, found: {csvs}")

            with tempfile.TemporaryDirectory() as work: # 빈폴더를 만드는데 이름은 겹치지 않게 무작위
                csv_path = zf.extract(csvs[0], work) #zip 안에서 csv 하나만 꺼내서 work 폴더에 품

                # Parquet 쓰기 + 검증
                src = f"read_csv('{csv_path}', all_varchar=true)"
                n_csv = duckdb.sql(f"SELECT count(*) FROM {src}").fetchone()[0]
                duckdb.sql(f"""
                    COPY (
                        SELECT *,'{info['ingested_at']}' AS _landing_ingested_at, 
                                '{info["sha256"]}' AS _landing_sha256
                        FROM {src}
                    ) TO '{tmp}' (FORMAT parquet)
                """)
                n_pq = duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
                if n_pq != n_csv:
                    raise ValueError(f"Row count mismatch: csv={n_csv}, parquet={n_pq}")

        os.replace(tmp, final)
        return str(final)
        

    
    to_bronze(download_bts())


bts_landing()