from datetime import datetime, timezone
from pathlib import Path
import shutil
import urllib.request

from airflow.sdk import dag, task

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

        folder = (
            LANDING_ROOT / "bts"
            / f"year={year}"
            / f"month={month:02d}"            # 우리 폴더: 0 채움 (01)
            / f"ingested_at={ingested_at}"
        )
        folder.mkdir(parents=True, exist_ok=False)

        url = BTS_URL.format(year=year, month=month)   # BTS 주소: 0 없음 (1)
        target = folder / url.rsplit("/", 1)[-1]        # 주소의 마지막 / 뒤 = BTS 파일 이름 그대로

        with urllib.request.urlopen(url) as resp, open(target, "wb") as f:
            shutil.copyfileobj(resp, f)

        return str(target)

    download_bts()


bts_landing()