# Webtoon Pipeline
webtoon-pipeline은 웹툰 데이터를 처리하는 Airflow DAGs입니다. 이 파이프라인은 웹툰 데이터를 크롤링하고, Spark를 사용해 데이터를 변환한 후, S3, Redshift, RDS에 데이터를 적재하는 과정을 자동화합니다. 

- 크롤링: webtoon-crawler 모듈을 호출하여 웹툰 데이터를 크롤링합니다.
- 데이터 변환: Spark를 사용해 데이터를 변환하고, 필요한 형태로 가공합니다.
- 데이터 적재: 처리된 데이터를 S3, Redshift, RDS 등의 데이터 저장소에 적재합니다.
- Airflow DAG 관리: 모든 작업 흐름을 Airflow DAGs로 정의하여, 자동화된 데이터 파이프라인을 운영할 수 있습니다.

## Setting Up
파이프라인을 원활하게 실행하려면 Variables와 Connections를 올바르게 등록해야 합니다.

### Variables
1. `aws_access_key`
    - AWS API에 접근하기 위한 액세스 키입니다. AWS 서비스에 접근할 때 필요합니다.
2. `aws_secret_key`
    - AWS API에 접근하기 위한 비밀 키입니다. AWS 서비스에 접근할 때 필요합니다.

### Connections
1. `aws_default`
    - AWS 서비스와 연결하기 위한 기본 커넥션입니다. S3, Redshift 등 AWS 리소스를 사용하는 데 필요합니다.
2. `redshift_default`
    - Redshift 데이터베이스와 연결하기 위한 커넥션입니다. Redshift에서 데이터를 쿼리하고 적재할 때 사용됩니다.
3. `spark_default`
    - Apache Spark 클러스터와 연결하기 위한 커넥션입니다. Spark 작업을 실행하기 위해 필요합니다.
4. `slack_default`
    - Slack과의 통합을 위한 커넥션입니다. 작업 상태나 알림을 Slack 채널로 전송할 때 사용됩니다.

## DAGs
DAG는 매일 지정된 시간에 자동으로 실행되며, 모든 작업 흐름을 Airflow DAGs로 관리합니다.
과거 데이터는 데이터 양이 많기 때문에, 차후 천천히 진행될 예정입니다.

1. `fetch_and_store_<platform>_daily_data`
    - 웹툰 데이터를 매일 수집하여 원시 데이터를 저장소에 저장
    - 트리거 시점: 한국 시간(KST) 기준 매일 12시에 자동으로 실행
2. `optimize_<platform>_daily_data`
    - 수집된 대용량 원시 데이터를 분석에 용이하게 최적화 진행
    - 트리거 시점: `fetch_and_store_<platform>_daily_data`가 성공적으로 완료된 후 자동으로 실행
2. `process_<platform>_daily_data`
    - 최적화된 원시 데이터를 분석하고 필요한 형태로 변환
    - 트리거 시점: `optimize_<platform>_daily_data`가 성공적으로 완료된 후 자동으로 실행
3. `load_<platform>_daily_data`
    - 변환된 데이터를 최종 데이터 웨어하우스 또는 데이터베이스에 로드
    - 트리거 시점: `process_<platform>_daily_data`가 완료된 후 자동으로 실행
4. `execute_dbt_analytics`
    - 웨어하우스의 데이터를 실사용할 데이터로 변환
    - 트리거 시점: `load_<platform>_daily_data`가 완료된 후 자동으로 실행 (현재 미지정)

### Init Dag
이 DAG들은 최초 프로젝트 시작 시에 한 번만 실행되며, 이후에는 재실행하지 않아도 됩니다.

1. `init_<platform>_historical_data`
    - 과거부터 현재까지 모든 웹툰 데이터를 수집
2. `init_redshift_external_tables`
    - Redshift 외부 데이터베이스 및 테이블을 생성

### DAG Flow
이 모든 작업은 순차적으로 트리거가 걸려 실행되며 흐름은 다음과 같습니다.

```
fetch_and_store_daily_data > optimize_daily_data > process_daily_data > load_daily_data > execute_dbt_analytics
```

## Commit Convention
webtoon-pipeline의 커밋 메시지는 기능/모듈명과 세부 내용을 포함해 아래와 같은 형식으로 작성합니다. 이를 통해 각 작업 흐름이나 기능이 추가된 부분을 쉽게 파악할 수 있습니다.

```
<기능/모듈명>:: <커밋 내용>
```

예시:
- dags:: 웹툰 크롤링 태스크 추가
- dags:: Spark 데이터 변환 로직 추가
- dags:: 데이터 적재 태스크 추가 (S3, Redshift, RDS)
- dags:: Airflow DAGs 초기 설정
