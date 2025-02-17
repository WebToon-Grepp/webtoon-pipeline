# Webtoon Pipeline
webtoon-pipeline은 웹툰 데이터를 처리하는 Airflow DAGs입니다. 이 파이프라인은 웹툰 데이터를 크롤링하고, Spark를 사용해 데이터를 변환한 후, S3, Redshift, RDS에 데이터를 적재하는 과정을 자동화합니다. 

- 크롤링: webtoon-crawler 모듈을 호출하여 웹툰 데이터를 크롤링합니다.
- 데이터 변환: Spark를 사용해 데이터를 변환하고, 필요한 형태로 가공합니다.
- 데이터 적재: 처리된 데이터를 S3, Redshift, RDS 등의 데이터 저장소에 적재합니다.
- Airflow DAG 관리: 모든 작업 흐름을 Airflow DAGs로 정의하여, 자동화된 데이터 파이프라인을 운영할 수 있습니다.

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
