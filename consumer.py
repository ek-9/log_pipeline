import os
import json
import time
import datetime
import pendulum
from kafka import KafkaConsumer
from conf.config import KAFKA_CONFIG, DATALAKE_CONFIG

KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG["bootstrap_servers"]
KAFKA_TOPIC             = KAFKA_CONFIG["topic"]
DATALAKE_PATH           = DATALAKE_CONFIG["path"]

# 카프카 컨슈머 준비 대기
def get_kafka_consumer():
    max_retries = 15
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC, # 읽을 토픽
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='earliest',  # 구독 전 못 받은 데이터부터 전부 받기
                enable_auto_commit=True, # 자동으로 offset 저장
                group_id='web-events-log-group', # 컨슈머 그룹 이름
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            # group_id로 offset 관리 가능
            return consumer
        except Exception as e:
            print(f"컨슈머 연결 대기 ({attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise Exception("컨슈머 연결 불가")

def append_to_datalake(event):

    # ----------------------------------------
    # [운영용] KST 기준 YYYYMMDDHH 포맷 (시간 단위)
    # hour_str = pendulum.now("Asia/Seoul").strftime("%Y%m%d%H")

    # TEST code
    # KST 기준 YYYYMMDDHHMM 포맷 (5분 버킷 단위)
    now = pendulum.now("Asia/Seoul")
    minute_5 = (now.minute // 5) * 5
    hour_str = now.strftime("%Y%m%d%H") + f"{minute_5:02d}"

    # -----------------------------------------

    os.makedirs(DATALAKE_PATH, exist_ok=True)
    filepath = os.path.join(DATALAKE_PATH, f"events_{hour_str}.jsonl")

    with open(filepath, 'a', encoding='utf-8') as f: # file append모드 -> 있으면 추가 없으면 파일 생성
        f.write(json.dumps(event, ensure_ascii=False) + '\n')

    return hour_str

if __name__ == "__main__":
    print("Kafka 컨슈머를 초기화합니다...")
    consumer = get_kafka_consumer()
    print(f"[{KAFKA_TOPIC}] 토픽 구독 시작. 이벤트를 DataLake에 적재합니다...")

    try:
        for message in consumer:
            event = message.value
            hour_str = append_to_datalake(event)
            print(f"[Consumer] {event['timestamp']} | {event['event_type']:>9} | → events_{hour_str}.jsonl")

    except KeyboardInterrupt:
        print("컨슈머를 종료합니다.")
    finally:
        consumer.close()
        print("Kafka 컨슈머 연결이 안전하게 종료되었습니다.")
