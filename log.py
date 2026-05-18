import os
import uuid
import random
import time
import json
from datetime import datetime, timedelta
import pendulum
from kafka import KafkaProducer
from conf.config import KAFKA_CONFIG

KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG["bootstrap_servers"]
KAFKA_TOPIC             = KAFKA_CONFIG["topic"]

# 카프카 프로세스 준비 대기
def get_kafka_producer():
    max_retries = 15
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            print(f"Kafka 연결 대기 중... ({attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    
    raise Exception("Kafka 서버에 연결할 수 없습니다. 설정을 확인해주세요.")

def generate_events(is_historical=False):
    PLATFORM = ["IOS", "ANDROID", "WINDOW", "MAC"]
    ENDPOINTS = ["/home", "/products", "/cart", "/order", "/login"]
    
    # 에러발생 확률 10%
    is_error = random.random() < 0.1 
    
    platform = random.choice(PLATFORM)
    endpoint = random.choice(ENDPOINTS)
    
    user_id = f"user_{random.randint(1, 100)}"
    
    if is_historical:
        # 초기 데이터 1000개는 최근 2일 내의 랜덤 시간으로 설정
        random_seconds_ago = random.randint(0, 2 * 24 * 60 * 60)
        base_time = pendulum.now("Asia/Seoul") - timedelta(seconds=random_seconds_ago)
    else:
        # 현재 시간을 활용한 실시간 데이터 생성
        base_time = pendulum.now("Asia/Seoul")
    
    events_to_return = []
    
    # page_view 이후 event_type 로그 저장
    base_pageview = {
        "event_id": str(uuid.uuid4()),
        "timestamp": base_time.isoformat(),
        "user_id": user_id,
        "platform": platform,
        "event_type": "PAGE_VIEW",
        "details": {
            "endpoint": endpoint,
            "status_code": 200
        }
    }
    events_to_return.append(base_pageview)
    
    # event_type은 페이지뷰 직후 발생하므로 0.05초~0.3초 미세 딜레이를 더함
    action_time = (base_time + timedelta(milliseconds=random.randint(50, 300))).isoformat()
    
    if is_error:
        # 에러 발생 시 (페이지뷰 이후 에러 로그가 찍힘)
        error_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": action_time,
            "user_id": user_id,
            "platform": platform,
            "event_type": "ERROR",
            "details": {
                "endpoint": endpoint,
                "status_code": random.choice([403, 404, 500]),
                "msg": "Connection Timeout" if random.random() < 0.5 else "Internal Server Error"
            }
        }
        events_to_return.append(error_event)
    else:
        # 정상 페이지 접근 시 추가 액션(이벤트) 로그가 생성됩니다.
        if endpoint == "/login":
            login_event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": action_time,
                "user_id": user_id,
                "platform": platform,
                "event_type": "LOGIN",
                "details": {
                    "endpoint": endpoint,
                    "status_code": 200,
                    "method": "POST"
                }
            }
            events_to_return.append(login_event)
        elif endpoint == "/order":
            purchase_event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": action_time,
                "user_id": user_id,
                "platform": platform,
                "event_type": "PURCHASE",
                "details": {
                    "endpoint": endpoint,
                    "amount": random.randrange(1000, 100000, 100),
                    "item_id": f"item_{random.randint(1, 50)}",
                    "status_code": 200
                }
            }
            events_to_return.append(purchase_event)
            
    return events_to_return

if __name__ == "__main__":
    print("Kafka 프로듀서 구동 대기 중...")
    try:
        producer = get_kafka_producer() # kafka 연결
        print("Kafka 프로듀서 연결 성공!")
    except Exception as e:
        print(f"Kafka 프로듀서 연결 실패: {e}")
        exit(1)
        
    print("초기 데이터 약 1000건을 생성하여 Kafka로 전송(Produce)합니다... (최근 2일간 쌓인 과거 데이터)")
    inserted_count = 0
    target_count = 1000
    while inserted_count < target_count:
        events = generate_events(is_historical=True) # 과거 로그 데이터 생성
        for ev in events:
            producer.send(KAFKA_TOPIC, ev)
            inserted_count += 1
            if inserted_count >= target_count:
                break
    producer.flush()
    print("과거 데이터 생성완료")
    print("===========================")
    print("실시간 데이터 생성 시작")
    try:
        while True:
            # 0.33 ~ 0.5초 간격으로 실시간 로그 데이터 생성
            time.sleep(random.uniform(0.33, 0.5))
            
            events = generate_events(is_historical=False) # 현재 실시간 로그 데이터 생성
            for ev in events:
                producer.send(KAFKA_TOPIC, ev)
            producer.flush()
            # print("로그 저장")
            
    except KeyboardInterrupt:
        print("로그 생성을 종료")
    finally:
        producer.close()
        print("kafka 종료")