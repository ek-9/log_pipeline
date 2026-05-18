import os
import json
import glob
import shutil
from datetime import datetime, timedelta
import pendulum
import psycopg2
from psycopg2.extras import execute_values, Json
from airflow import DAG
from airflow.operators.python import PythonOperator
from conf.config import DATALAKE_CONFIG, DB_CONFIG

DATALAKE_PATH  = DATALAKE_CONFIG["path"]
PROCESSED_PATH = os.path.join(DATALAKE_PATH, "processed")

def create_table_if_not_exists():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events_log (
                        id         SERIAL PRIMARY KEY,
                        event_id   UUID        UNIQUE NOT NULL,
                        timestamp  TIMESTAMP   NOT NULL,
                        user_id    VARCHAR(50),
                        event_type VARCHAR(50),
                        platform   VARCHAR(50),
                        details    JSONB
                    );
                """)
        print("events_log 테이블 생성 완료")
    finally:
        conn.close()


def get_target_files():

    # 시간 단위 파일 (KST 기준 - Consumer와 동일)
    # now = pendulum.now("Asia/Seoul")
    # current_bucket = now.strftime("%Y%m%d%H")

    # TEST
    # 5분 단위 : YYYYMMDDHHMM
    now = pendulum.now("Asia/Seoul")
    minute_bucket = (now.minute // 5) * 5
    current_bucket = now.strftime("%Y%m%d%H") + f"{minute_bucket:02d}"

    all_files = sorted(glob.glob(os.path.join(DATALAKE_PATH, "events_*.jsonl"))) # 전체파일 가져와서

    # 현재 쓰고있는 파일은 제외
    target_files = []
    for f in all_files:
        if os.path.basename(f) != f"events_{current_bucket}.jsonl":
            target_files.append(f)

    print(f"전체 파일: {len(all_files)}개 | 처리 대상: {len(target_files)}개 (현재 버킷 {current_bucket} 제외)")
    return target_files


# 파일을 읽어 DB에 저장하고 처리된 파일은 procceed로 이동
def load_events_to_postgres():
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    target_files = get_target_files()

    if not target_files:
        print("처리할 파일이 없습니다. 종료합니다.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    total_inserted = 0
    total_skipped  = 0

    try:
        for filepath in target_files:
            filename = os.path.basename(filepath)
            print(f"\n[처리 시작] {filename}")

            batch = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        batch.append((
                            event["event_id"],
                            event["timestamp"],
                            event["user_id"],
                            event["event_type"],
                            event["platform"],
                            Json(event["details"]),
                        ))
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"  ⚠ 파싱 오류 (스킵): {e} | 내용: {line[:80]}")

            if not batch:
                print(f"  → 유효한 이벤트 없음. 파일 이동 후 다음으로.")
                _move_to_processed(filepath)
                continue

            # INSERT
            with conn:
                with conn.cursor() as cur:

                    execute_values(cur,
                    """
                        INSERT INTO events_log
                            (event_id, timestamp, user_id, event_type, platform, details)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING
                    """,
                    batch)

            # 완료 파일은 processed로 이동
            _move_to_processed(filepath)

    finally:
        conn.close()

    print("배치 완료")



def _move_to_processed(filepath):
    dest = os.path.join(PROCESSED_PATH, os.path.basename(filepath))
    shutil.move(filepath, dest)


# DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="load_web_events_to_postgres",
    default_args=default_args,
    description="JSONL to postgresql",

    schedule_interval="10 */6 * * *",  # 6시간에 한번 배치
    # schedule_interval="5/10 * * * *",  # TEST: 5분마다

    start_date=datetime(2026, 4, 22, tzinfo=kst), # 시작 날짜 KST 기준
    catchup=False, # 과거 DAG스케줄 실행할지? -> 안함
    tags=["events_log", "batch", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    t2 = PythonOperator(
        task_id="load_events_to_postgres",
        python_callable=load_events_to_postgres,
    )


    t1 >> t2
