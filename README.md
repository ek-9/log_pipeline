# 이벤트 로그 파이프라인 구축

## 목표
이벤트를 생성하고 → 저장하고 → 분석하고 → 시각화하는 파이프라인을 구축한다.  

## Skill
Python, kafka, airflow, postgresql, Grafana, Docker, Docker compose  

## 전체 구조
Event Generator → Kafka → JSONL Data Lake → Airflow (ETL) → PostgreSQL → Analysis / Grafana  

## 필수 도구
Docker, Docker comppose

## 실행 방법

```
git clone <repo-url>  
cd liveklass  
```
1. 폴더 접근

```
cd (다운받은 경로)/liveklass  
```

2. Docker 실행  

```
docker compose up --build
```  
만약 포트가 사용중이라면 다른 포트로 변경해주세요.

예시)  
```
    container_name: grafana
    ports:
      - "3000:3000" # <- 이부분
```
3. Airflow UI 접속  

http://localhost:8080  
	•	ID: admin  
	•	PW: admin  
	•	DAG 최초 실행 시 Unpause 필요  
	•	이후 6시간마다 자동 실행  

4. 분석 리포트
```
python -m analysis.analysis
```

## 스키마 설명
-   id SERIAL PRIMARY KEY,          로그id(기본키),  
-   event_id UUID UNIQUE NOT NULL,  UUID기반 중복문제 해결,  
-   timestamp TIMESTAMP NOT NULL,   로그 발생 시간  
-   user_id VARCHAR(50),            사용 user_id  
-   event_type VARCHAR(50),         발생한 이벤트 타입  
-   platform VARCHAR(50),           접속한 플랫폼  
-   details JSONB                   이벤트 타입에 따른 세부정보  

1) 중요한 정보(timestamp, user_id, event_type)는 column으로 저장했습니다.  
2) 이벤트 타입에 따른 세부사항이 다르기 때문에 JSONB기반 column을 설계했습니다.  
3) event_id는 UUID를 기반으로 해 Airflow를 통해 DB로 저장되는 과정에 중복이 발생할 수 있는 문제를 해결했습니다.  

## 구현하면서 고민한점

1. 데이터파이프라인의 구성

로컬 디렉토리에 JSONL파일로 로그를 저장했습니다.  
- 이러한 방식은 RDBMS에 저장하는 것 대비 저장 용량이 크고 비용적인 장점을 갖습니다.  
- docker를 통해 컨테이너 안에서 한번에 구동하게 하기 위해 서버기반 S3대신 활용하였습니다.  
- 실제 운영방식에서 S3를 활용하고자 합니다.  

로그데이터의 배치 처리와 흐름 관리를 위해 Airflow를 활용했습니다.  
- Airflow를 통해 ETL실행 순서를 정확히 관리하고 안정적인 배치처리를 수행해 대용량 데이터 저장에 있어서 처리 효율을 높이고 시스템 부하를 낮추기 위함입니다.  

실시간의 장점을 살리기 위해 kafka를 기반으로 로그를 수집했습니다.
- 실시간으로 서비스병목, 부하 등을 확인할 수 있도록 kafka를 통해 실시간 데이터를 json파일에 저장했습니다.  
- 이러한 구조는 향후 새로운 consumer를 통해 실시간 모니터링 등의 서비스를 확장하는 것에 유리하다고 판단하였습니다.  

2. 데이터 정합성  
event_id에 unique제약을 걸고 ON CONFLICT DO NOTHING를 통해 배치작업 도중 이벤트가 중복으로 저장될 경우를 대비했습니다.  

3. 운영
하나의 컨테이너에 구성하고 grafana dashboard를 json파일로 저장하여 재배포 시에도 자동으로 대시보드가 적용되도록 설계했습니다.  
