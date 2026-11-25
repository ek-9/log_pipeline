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


## 로그 발생기

PLATFORMS = ["IOS", "ANDROID", "WINDOWS", "MAC"]
ENDPOINTS = [
    "/home",
    "/products",
    "/cart",
    "/order",
    "/login"
]


ERROR_CODES = {
    "GET": [404, 500],
    "POST": [400, 401, 500]
}
ERROR_TYPES = {
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    500: "Internal Server Error"
}


def generate_events(is_historical=False):

    platform = random.choice(PLATFORMS)
    user_id = f"user_{random.randint(1, 100)}"

    if is_historical:
        base_time = pendulum.now("Asia/Seoul").subtract(
            seconds=random.randint(0, 172800)
        )
    else:
        base_time = pendulum.now("Asia/Seoul")

    events_to_return = []

    # 공통 이벤트 생성 함수
    def create_event(event_type, endpoint, method, event_time, details=None):
        # 에러 발생확률 : 10%
        is_error = random.random() < 0.1

        if is_error:
            status_code = random.choice(ERROR_CODES[method])
        else:
            status_code = 201 if method == "POST" else 200

        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": event_time.isoformat(),
            "user_id": user_id,
            "platform": platform,
            "event_type": event_type,
            "details": {
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code
            }
        }

        if is_error:
            event["details"]["error_type"] = ERROR_TYPES[status_code]

        if details:
            event["details"].update(details)

        events_to_return.append(event)

        return not is_error

    # 1. GET으로 PAGE_VIEW
    endpoint = random.choice(ENDPOINTS)
    page_success = create_event("PAGE_VIEW", endpoint, "GET", base_time)

    # PAGE_VIEW 실패 시 후속 이벤트 없음
    if not page_success:
        return events_to_return

    # 2. LOGIN
    # 로그인 페이지 조회 후 로그인 요청
    if endpoint == "/login":
        action_time = base_time.add(milliseconds=random.randint(50, 300))
        create_event("LOGIN", "/login", "POST", action_time)
        return events_to_return

    # 3. 구매
    if endpoint == "/products":
        action_time = base_time.add(milliseconds=random.randint(50, 300))

        # 상품 조회 후 일부 사용자만 장바구니 추가
        if random.random() < 0.5:
            # 상품 선택
            item_id = f"item_{random.randint(1, 50)}"
            cart_success = create_event("ADD_TO_CART", "/cart", "POST", action_time, {"item_id": item_id})

            # 장바구니 요청 실패 시 퍼널 진행 종료
            if not cart_success:
                return events_to_return

            # 장바구니 추가 후 일부 구매
            if random.random() < 0.5:
                order_time = action_time.add(milliseconds=random.randint(50, 300))
                create_event(
                    "PURCHASE",
                    "/order",
                    "POST",
                    order_time,
                    {
                        "amount": random.randrange(1000, 100000, 100),
                        "item_id": item_id
                    }
                )

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