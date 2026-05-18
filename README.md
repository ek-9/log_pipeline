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

## 시각화

<img width="1430" height="719" alt="스크린샷 2026-04-22 오후 11 50 52" src="https://github.com/user-attachments/assets/58f54cd8-79db-4deb-ae8b-b1f434957f5d" />


## DB
**Local JSONL**  
이유?  

**PostgreSQL**  
구조화된 테이블을 통해 분석에 필요한 데이터를 SQL을 통해 추출할 수 있도록 구성했습니다.  

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
- 과제에는 실시간 모니터링의 요구사항은 없었지만 실시간으로 서비스병목, 부하 등을 확인할 수 있도록 kafka를 통해 실시간 데이터를 json파일에 저장했습니다.  
- 이러한 구조는 향후 새로운 consumer를 통해 실시간 모니터링 등의 서비스를 확장하는 것에 유리하다고 판단하였습니다.  

2. 데이터 정합성  
event_id에 unique제약을 걸고 ON CONFLICT DO NOTHING를 통해 배치작업 도중 이벤트가 중복으로 저장될 경우를 대비했습니다.  

3. 데이터 분석
DAG에 포함되지 않은 python script를 활용해 분석 리포트를 작성했습니다.  
- 데이터마트를 새로 설계하는 방식보다는 파이썬 스크립트를 통해 분석 리포트를 제공할 수 있도록 구현했습니다.  
- 에러현황, 사용자 서비스이용 현황과 같은 지표는 장기적인 값보다 단기간의 최근 정보를 파악하는 것이 더 중요하다고 판단했기 때문입니다.
- 이러한 구현 방식은 아직은 터미널상 출력에 그치지만 이 데이터를 활용해 API와 연계된 서비스를 개발할 수 있을 것이라 판단했습니다.  

Grafana를 활용해 시각화했습니다.
- 업데이트를 주기적으로 모니터링 할 수 있기때문입니다.  
- 별도의 데이터 구성 없이 필요한 정보를 쿼리를 통해 추출가능하다는 장점이 있습니다.  

4. 운영
하나의 컨테이너에 구성하고 grafana dashboard를 json파일로 저장하여 재배포 시에도 자동으로 대시보드가 적용되도록 설계했습니다.  

## Optional B

<img width="777" height="314" alt="스크린샷 2026-04-22 오후 11 10 40" src="https://github.com/user-attachments/assets/1e700b55-7d90-4cba-ab3b-c076591506b6" />

### 아키텍처 설명 및 사용 이유

1. MSK : kafka를 위한 브로커, 클러스터 관리가 유용하며 로그 데이터는 실시간 대용량로 발생하므로 이를 관리하기 위한 서버를 따로 구축할 것 같습니다.
2. S3 : 파일기반의 저장을 통해 데이터레이크를 구성해 로그데이터의 대용량 저장에 있어 확장성과 비용적인 부분을 확보합니다.  
이를 통해 로그데이터 원본을 그대로 저장하면서 이후 활용을 용이하게 할 수 있습니다.  
3. EC2 : 현재 단계에서 airflow를 활용한 DAG의 구조가 간단하기 떄문에 grafana와 함께 EC2의 하나의 인스턴스에 배포합니다.  
이를 통해 운영 복잡도를 줄이고 비용 부담을 낮출 수 있을 것이라 판단했습니다.  
4. RDS : RDBMS의 성능 평가에 용이하고 DB의 확장성, 안정성을 높일 수 있습니다.

### 가장 고민한 부분
각 서비스에 맞는 서비스가 분명히 존재하지만 향후 활용과 비용간의 균형을 가장 많이 고민하였습니다.    
기본적으로 구성이 간단한 시각화, DAG부분은 EC2에 하나로 배포하는 것이 효율적이라 생각하였고 postgresql은 DB의 중요도를 고려하여 백업과 보안의 효과가 좋은 RDS에 배포할 것을 선택했습니다.  
또한 로그데이터는 실시간으로 계속해서 데이터 양이 증가하기 때문에 이를 고려하여 서비스를 따로 선택하여 배포할 것을 생각했습니다.  
