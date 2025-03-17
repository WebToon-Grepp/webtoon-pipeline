WITH webtoon_engagement AS (
    -- 웹툰별 총 조회수(views), 총 회차 수(episode_count) 계산
    SELECT
        t.id AS title_id,
        t.title,
        t.author,
        t.platform,
        COALESCE(SUM(t.views), 0) AS total_views,  -- 총 조회수
        COUNT(e.id) AS episode_count  -- 총 회차 수
    FROM raw_data.titles t
    LEFT JOIN raw_data.episodes e ON t.id = e.title_id  -- 회차 데이터 결합
    WHERE t.platform = 'naver'  -- 네이버 웹툰만 선택
    GROUP BY t.id, t.title, t.author, t.platform
)
-- TOP 10 웹툰을 회차당 평균 조회수 기준으로 선정
SELECT *,
    ROUND(total_views * 1.0 / NULLIF(episode_count, 0), 2) AS avg_views_per_episode
FROM webtoon_engagement
ORDER BY avg_views_per_episode DESC
LIMIT 10