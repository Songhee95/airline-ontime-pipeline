from datetime import datetime, timezone #지금 시간, utc 시간대
from pathlib import Path #파일/폴더 경로를 다루는 도구
import shutil # 다운로드 내용을 파일로 복사
import urllib.request #인터넷에서 파일 받기
import hashlib # sha256 지문 계산
import json #lineage 를 json 글자로 바꾸기
import os # os.rename 이름 바꾸기
import zipfile # zip 인지 아닌지 검사
import urllib.error # 404 구분용 HTTPError

from airflow.sdk import dag, task, get_current_context

BTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
LANDING_ROOT = Path("/opt/airflow/data/landing/")  
BRONZE_ROOT = Path("/opt/airflow/data/bronze/")
START_MONTH = (2025, 1) # Landing 이 비어있을때 시작할 달
MAX_MONTHS_PER_RUN = 3 # 한 run 에 최대 몇달까지 받을수 잇나

# Find the watermark (lowerbound from manifest.json)
def find_watermark(landing_root:Path) -> tuple[int,int] | None: 
    """Return the latest (year, month) that has a completed version (_manifest.json). None if nothing."""
    done = set()
    for m in landing_root.glob('bts/year=*/month=*/ingested_at=*/_manifest.json'):
        year_dir = m.parent.parent.parent.name
        month_dir = m.parent.parent.name
        year = int(year_dir.split('=')[1])
        month = int(month_dir.split('=')[1])
        done.add((year, month))
    return max(done, default=None)

# 하나씩 순서대로 받고, skip 이 안되는 구조 (빈달이 생길수 없는 구조) 로 만들기
def next_month(ym: tuple[int, int]) -> tuple[int, int]: 
    """(2025, 8) -> (2025, 9) , (2025, 12) -> (2026, 1)"""
    year, month = ym
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)

def download_month(year:int, month:int) -> dict | None: 
    """Download one BTS month into Landing. Return None if BTS has not published it yet (404)"""
    ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    month_dir = LANDING_ROOT / 'bts' / f"year={year}" / f"month={month:02d}"
    final_dir = month_dir / f"ingested_at={ingested_at}"
    tmp_dir = month_dir / f"_tmp_ingested_at={ingested_at}"
    
    url = BTS_URL.format(year=year, month=month)
    zip_name = url.rsplit("/", 1)[-1]
    tmp_zip=tmp_dir / zip_name

    # BTS 접속 먼저 (404 아직 공개 안됨 -> None, 그 외 에러 -> raise)
    try: 
        resp = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        if e.code == 404: 
            return None 
        raise 

    # 접속 성공 뒤에만 폴더를 만듦 
    with resp: 
        tmp_dir.mkdir(parents=True, exist_ok=False)
        with open(tmp_zip, 'wb') as f:  # 작업 공간에 파일을 쓰기(w) + 바이너리(b) 모드로 엶
            shutil.copyfileobj(resp, f) #응답을 조금씩 읽어서 파일에 씀
        
    # 파일 download atomic (다운로드가 끝까지 성공 해야만 최종 폴더가 생김)
    if not zipfile.is_zipfile(tmp_zip): 
        raise ValueError(f"Downloaded file is not a valid zip: {url}")
    
    # Lineage json file creation
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

# landing 은 완료 했는데 bronze 로 가는 과정에서 fail 이 난경우, manifest 데이터랑 bronze 파일을 비교해서 
# bronze 에 없는것들은 landing 부터 다운로드 하지말고, 있는걸 다시 Trigger
def latest_versions(landing_root: Path) -> dict[tuple[int, int], Path]: 
    """
    For each (year, month), return the latest completed version folder 
    (ingested_at=... with _manifest.json)
    """
    latest = {}
    for m in landing_root.glob('bts/year=*/month=*/ingested_at=*/_manifest.json'): 
        version_dir = m.parent # /opt/airflow/data/landing/bts/year=2025/month=01/ingested_at=20261019T235939Z
        year = int(m.parent.parent.parent.name.split('=')[1])
        month = int(m.parent.parent.name.split('=')[1])
        ym = (year, month)
        # version_dir.name => ingested_at=20261019T235939Z
        if ym not in latest or version_dir.name > latest[ym].name: # ingested_at 이 가장 최신것만 꺼냄. 
            latest[ym] = version_dir
    return latest 

# 이 bronze 파일은 어느 landing 버전으로 만들었나. 파일이 없으면 None
def bronze_source(year: int, month:int) -> str | None: 
    """
    Return the _landing_ingested_at that Bronze (year, month) was built from. 
    None if there is no Bronze file.
    """
    import duckdb
    path = BRONZE_ROOT / 'bts' / f"p_year={year}" / f"p_month={month:02d}" / "data.parquet"
    if not path.exists():
        return None
    with duckdb.connect(config={"memory_limit": "1GB"}) as con: 
        # 데이터 안에 _landing_ingested_at 컬럼에 출처가 1개인지를 confirm
        rows = con.sql(f"SELECT DISTINCT _landing_ingested_at FROM read_parquet('{path}')").fetchall()
    if len(rows) != 1: 
        raise ValueError(f"Bronze {path} should come from exactly 1 Landing version, found: {rows}")
    return rows[0][0]



@dag(schedule=None, catchup=False)
def bts_landing():
    @task
    def download_new_months() -> list[dict]:
        wm = find_watermark(LANDING_ROOT)
        ym = START_MONTH if wm is None else next_month(wm)
        results = []
        for _ in range(MAX_MONTHS_PER_RUN): # 최대 3번 반복
            info = download_month(*ym) # *ym  = (2025,9) 를 풀어서, year = 2025, month=9 로 넘김
            if info is None: 
                break
            results.append(info)
            ym=next_month(ym)
        return results

    @task
    def plan_bronze() -> list[dict]: 
        """
        Months whose Bronze is missing or built from an older Landing version (reconcile)
        """
        plan = []
        for (year, month), version_dir in sorted(latest_versions(LANDING_ROOT).items()):
            manifest = json.loads((version_dir / "_manifest.json").read_text())
            want = manifest['ingested_at']
            have = bronze_source(year, month)
            if want != have: 
                zip_name = manifest['source_url'].rsplit('/', 1)[-1]
                plan.append({
                    'zip_path': str(version_dir / zip_name), 
                    'year': year,
                    'month': month,
                    'ingested_at': want, 
                    'sha256': manifest['sha256']
                })
        return plan
            

    @task(max_active_tis_per_dagrun=1) # 한 run 안에서 to_bronze 는 한번에 1개씩만
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
                with duckdb.connect(config={"memory_limit":"1GB"}) as con: 
                    print("duckdb memory_limit =", con.sql("SELECT current_setting('memory_limit')").fetchone()[0])
                    n_csv = con.sql(f"SELECT count(*) FROM {src}").fetchone()[0]
                    con.sql(f"""
                        COPY (
                            SELECT *,'{info['ingested_at']}' AS _landing_ingested_at, 
                                    '{info["sha256"]}' AS _landing_sha256
                            FROM {src}
                        ) TO '{tmp}' (FORMAT parquet)
                    """)
                    n_pq = con.sql(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
                    if n_pq != n_csv:
                        raise ValueError(f"Row count mismatch: csv={n_csv}, parquet={n_pq}")

        os.replace(tmp, final)
        return str(final)
        

    plan = plan_bronze()
    download_new_months() >> plan    # 순서만: Landing을 다 받은 다음에 계획 세우기
    to_bronze.expand(info=plan)       # 계획에 있는 달마다 to_bronze 하나씩


bts_landing()