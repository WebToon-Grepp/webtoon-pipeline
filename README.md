# Webtoon Pipeline
webtoon-pipeline은 웹툰 데이터를 처리하는 Airflow DAGs입니다. 이 파이프라인은 웹툰 데이터를 크롤링하고, Spark를 사용해 데이터를 변환한 후, S3, Redshift, RDS에 데이터를 적재하는 과정을 자동화합니다. 

- 크롤링: webtoon-crawler 모듈을 호출하여 웹툰 데이터를 크롤링합니다.
- 데이터 변환: Spark를 사용해 데이터를 변환하고, 필요한 형태로 가공합니다.
- 데이터 적재: 처리된 데이터를 S3, Redshift, RDS 등의 데이터 저장소에 적재합니다.
- Airflow DAG 관리: 모든 작업 흐름을 Airflow DAGs로 정의하여, 자동화된 데이터 파이프라인을 운영할 수 있습니다.

## DAGs
DAG는 매일 지정된 시간에 자동으로 실행되며, 모든 작업 흐름을 Airflow DAGs로 관리합니다.
과거 데이터는 데이터 양이 많기 때문에, 차후 천천히 진행될 예정입니다.

1. **fetch_and_store_naver_daily_data**
    - 동작 시간: `한국 시간 18시`
    - 역할: 당일 업로드된 웹툰 데이터를 파싱하고, Spark 정제 작업을 진행합니다.
2. **fetch_and_store_kakao_daily_data**
    - 동작 시간: `한국 시간 19시`
    - 역할: 당일 업로드된 웹툰 데이터를 파싱하고, Spark 정제 작업을 진행합니다.
3. **add_partition_redshift_table**
    - 동작 시간: `한국 시간 19시 30분`
    - 역할: Spark로 정제된 데이터를 Redshift 외부 테이블에 적재합니다.
4. **execute_dbt_analytics**
    - 동작 시간: `한국 시간 20시`
    - 역할: 위의 3개 작업이 완료된 후 실사용할 데이터를 변환하는 작업을 진행합니다. (현재는 실행되지 않음)
5. **fetch_and_store_naver_historical_data**
    - 동작 시간: `한 번만 실행`
    - 역할: 과거부터 현재까지 모든 웹툰 데이터를 파싱합니다. (Spark 정제 작업은 아직 진행되지 않음)
6. **fetch_and_store_kakao_historical_data**
    - 동작 시간: `한 번만 실행`
    - 역할: 과거부터 현재까지 모든 웹툰 데이터를 파싱합니다. (Spark 정제 작업은 아직 진행되지 않음)
7. **create_redshift_external_tables**
    - 동작 시간: `한 번만 실행`
    - 역할: Redshift 외부 데이터베이스 및 스키마/테이블을 생성합니다.

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
