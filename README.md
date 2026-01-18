# enjoy-workflow

Apache Airflow 기반의 데이터 워크플로우 및 ETL 파이프라인 프로젝트입니다. 다양한 데이터 소스로부터 데이터를 수집, 처리, 변환하고 실시간 스트리밍 처리를 지원합니다.

## 주요 기능

- **워크플로우 오케스트레이션**: Apache Airflow를 통한 자동화된 데이터 파이프라인 관리
- **다중 데이터 소스 연동**: MySQL, Elasticsearch, AWS S3, Kafka, Google Sheets 등
- **분산 데이터 처리**: Apache Spark와 Livy를 이용한 대용량 데이터 처리
- **데이터 품질 검증**: Great Expectations 및 Deequ를 통한 데이터 검증
- **실시간 스트리밍**: Kafka 및 Confluent Kafka를 이용한 메시지 처리
- **Change Data Capture**: Debezium을 이용한 MySQL CDC 및 Kafka Connect 연동
- **자동화된 배포**: GitHub Actions를 통한 CI/CD 파이프라인
- **모니터링**: Slack 알림을 통한 작업 상태 모니터링

## 기술 스택

### 핵심 기술
- **Python**: 3.12
- **Apache Airflow**: 3.1.5
- **Kafka**: kafka-python 2.3.0, confluent-kafka 2.13.0
- **JupyterLab**: 4.5.1

### 주요 라이브러리
- **데이터 처리**: pandas, numpy, DuckDB
- **데이터 품질**: Great Expectations, Deequ
- **웹 크롤링**: beautifulsoup4, newspaper3k, feedparser
- **클라우드**: boto3 (AWS), Google Cloud 프로바이더
- **데이터베이스**: psycopg2, elasticsearch
- **메시징**: fastavro, Schema Registry

## 프로젝트 구조

```
enjoy-workflow/
├── dags/                           # Airflow DAG 정의
│   ├── common/                     # 공통 유틸리티
│   │   ├── mmix_utils.py          # MySQL 연결 유틸
│   │   ├── mmix_slack_operator.py # Slack 알림
│   │   └── mmix_validator.py      # 데이터 검증
│   ├── example_connection.py       # 데이터 소스 연결 테스트
│   ├── example_news_crawling.py    # 뉴스 크롤링 파이프라인
│   ├── example_spark_mysql.py      # Spark MySQL 처리
│   ├── example_spark_deequ.py      # Spark 데이터 검증
│   ├── example_great_expectations.py # Great Expectations
│   └── example_s3_to_google.py     # S3 ↔ Google Sheets
│
├── notebooks/                      # Jupyter 노트북 예제
│   ├── host/                       # 로컬 실행 예제
│   │   ├── config/                       # Kafka Connect 설정
│   │   │   ├── mysql-source-connector.json  # Debezium MySQL CDC
│   │   │   └── s3-sync-connector.json       # Confluent S3 Sink
│   │   ├── example_kafka.ipynb           # Kafka JSON
│   │   ├── example_kafka_avro.ipynb      # Kafka Avro
│   │   ├── example_kafka_connet.ipynb    # Kafka Connect
│   │   ├── example_msk.ipynb             # AWS MSK
│   │   ├── example_elasticsearch.ipynb   # Elasticsearch
│   │   ├── example_mysql_replica_.ipynb  # MySQL Replication
│   │   ├── example_mysql_faker.ipynb     # 더미 데이터 생성
│   │   ├── example_pandas.ipynb          # Pandas 분석
│   │   └── example_duckdb.ipynb          # DuckDB 쿼리
│   └── airflow/                    # Airflow 실행 예제
│       └── example_variable.ipynb  # Airflow 변수 처리
│
├── requirements/
│   └── requirements.txt            # Python 의존성
│
├── startup_script/
│   └── startup.sh                  # 초기화 스크립트
│
└── .github/workflows/
    └── github-actions-prod.yml     # CI/CD 파이프라인
```

## 설치 방법

### 1. Conda 환경 생성

```bash
conda create -n enjoy-workflow python==3.12
conda activate enjoy-workflow
```

### 2. 의존성 설치

```bash
pip install jupyterlab kafka-python==2.3.0 apache-airflow==3.1.5 -r requirements/requirements.txt
```

## DAG 예제

### 1. Connection Test (example_connection.py)
다양한 데이터 소스에 대한 연결 테스트를 수행합니다.
- MySQL (Primary/Replication)
- Elasticsearch
- AWS S3
- Slack 알림 통합

### 2. News Crawling (example_news_crawling.py)
네이버 뉴스 API를 이용한 뉴스 크롤링 및 저장 파이프라인입니다.
- Google Sheets에서 키워드 다운로드
- 네이버 뉴스 API 검색
- 병렬 크롤링
- MySQL Aurora 저장
- Slack 요약 전송

### 3. Spark MySQL (example_spark_mysql.py)
Livy를 통한 Spark 작업 제출 및 MySQL 데이터 처리입니다.
- S3에서 Python 휠 참조
- Base64 인코딩 연결 정보 전달
- 30분마다 스케줄 실행

### 4. Data Quality (example_great_expectations.py)
Great Expectations를 이용한 데이터 품질 검증입니다.
- Faker로 더미 데이터 생성 (1,000~15,000행)
- 행 수, 고유성, NULL, 범위 검증
- 검증 결과 MySQL 로깅

### 5. Spark Deequ (example_spark_deequ.py)
Deequ 라이브러리를 이용한 Spark 데이터 검증입니다.
- Livy HTTP API 사용
- 배치 상태 폴링

### 6. S3 to Google Sheets (example_s3_to_google.py)
S3와 Google Sheets 간 데이터 동기화입니다.
- Parquet 포맷 지원
- 양방향 데이터 전송

## Jupyter 노트북 예제

### Kafka
- **example_kafka.ipynb**: Confluent Kafka를 이용한 JSON 메시지 송수신
  - AdminClient로 토픽 관리
  - Producer로 100,000개 메시지 전송
  - Faker로 더미 데이터 생성

- **example_kafka_avro.ipynb**: Avro 직렬화 포맷 처리
  - Schema Registry 활용
  - zstd 압축

- **example_kafka_connet.ipynb**: Kafka Connect 연동
  - Debezium MySQL CDC Source Connector (Change Data Capture)
  - Confluent S3 Sink Connector
  - Connector 생성, 조회, 삭제 API 관리
  - JSON 기반 커넥터 설정 파일 관리

- **example_msk.ipynb**: AWS MSK(Managed Streaming for Kafka) 연동

### 데이터베이스
- **example_elasticsearch.ipynb**: Elasticsearch 연동 및 쿼리
- **example_mysql_replica_.ipynb**: MySQL Replication 설정
- **example_mysql_faker.ipynb**: Faker로 더미 데이터 생성 및 MySQL 적재

### 데이터 분석
- **example_pandas.ipynb**: Pandas 데이터 조작 및 분석
- **example_duckdb.ipynb**: DuckDB SQL 쿼리

## CI/CD

GitHub Actions를 통한 자동 배포 파이프라인:

1. **Ready_Notify**: 배포 시작 Slack 알림
2. **Deploy**: AWS S3에 파일 동기화
   - dags/
   - requirements/
   - startup_script/
3. **Result_Notify**: 배포 결과 Slack 알림

워크플로우 파일: `.github/workflows/github-actions-prod.yml`

## 공통 유틸리티

### mmix_utils.py
MySQL 연결 JSON 빌더 및 유틸리티 함수

### mmix_slack_operator.py
Slack 알림을 위한 콜백 함수:
- `on_success_callback`: 작업 성공 시 알림
- `on_failure_callback`: 작업 실패 시 알림
- `on_skip_callback`: 작업 스킵 시 알림

### mmix_validator.py
Great Expectations를 이용한 데이터 검증 로직

## 주요 특징

### 엔터프라이즈급 아키텍처
- Airflow + Spark + Kafka 통합
- Livy를 통한 Spark 작업 제출
- 동적 리소스 할당

### 데이터 품질 관리
- Great Expectations를 통한 검증 규칙 정의
- Deequ를 이용한 Spark 데이터 검증
- 검증 결과 자동 로깅

### 실시간 메시징 및 CDC
- Confluent Kafka + Schema Registry
- JSON 및 Avro 직렬화
- zstd 압축 지원
- Kafka Connect를 통한 데이터 파이프라인 자동화
- Debezium MySQL CDC로 실시간 데이터베이스 변경 이벤트 캡처
- S3 Sink Connector로 스트리밍 데이터 저장

### 한글 지원
- 네이버 뉴스 API 연동
- Faker를 이용한 한글 더미 데이터 생성

## 최근 업데이트

### 2026년 1월
- **Kafka Connect 통합**: Debezium MySQL CDC Source Connector 및 S3 Sink Connector 추가
- **노트북 구조 개선**: `notebooks/docker/` → `notebooks/airflow/`로 디렉토리 재구성
- **데이터 분석 노트북 확장**: Pandas 및 DuckDB 예제 대폭 보강
- **설정 파일 관리**: Kafka Connect 커넥터 설정을 `config/` 디렉토리로 분리
- **의존성 업데이트**: requirements.txt 최신화

## 라이선스

이 프로젝트는 개인 학습 및 개발 목적으로 작성되었습니다.