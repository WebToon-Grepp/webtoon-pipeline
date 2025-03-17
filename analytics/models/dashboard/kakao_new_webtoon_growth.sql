WITH new_webtoons AS (
    -- 최근 30일 이내 연재를 시작한 신규 웹툰 필터링
    SELECT
        id AS title_id,
        title,
        author,
        platform,
        is_completed,
        TO_DATE(year::TEXT || '-' || LPAD(month::TEXT, 2, '0') || '-' || LPAD(day::TEXT, 2, '0'), 'YYYY-MM-DD') AS start_date
    FROM raw_data.titles
    WHERE platform = 'kakao'
    AND TO_DATE(year::TEXT || '-' || LPAD(month::TEXT, 2, '0') || '-' || LPAD(day::TEXT, 2, '0'), 'YYYY-MM-DD')
        >= DATEADD(day, -30, CURRENT_DATE)  -- INTERVAL 대신 DATEADD() 사용
),
daily_growth AS (
    -- 신규 웹툰의 일별 좋아요 & 댓글 증가량 계산
    SELECT
        e.title_id,
        e.updated_date,
        COALESCE(SUM(e.likes), 0) AS daily_likes,  -- NULL 방지
        COALESCE(SUM(e.comments), 0) AS daily_comments  -- NULL 방지
    FROM raw_data.episodes e
    RIGHT JOIN new_webtoons nw ON e.title_id = nw.title_id  -- LEFT JOIN → RIGHT JOIN으로 변경
    WHERE e.platform = 'kakao'
    GROUP BY e.title_id, e.updated_date
),
growth_summary AS (
    -- 웹툰별 총 좋아요/댓글 증가량 및 성장률 계산
    SELECT
        nw.title_id,
        nw.title,
        nw.author,
        nw.platform,
        MIN(nw.start_date) AS start_date,
        COALESCE(SUM(dg.daily_likes), 0) AS total_likes,  -- NULL 방지
        COALESCE(SUM(dg.daily_comments), 0) AS total_comments,  -- NULL 방지
        ROUND(COALESCE(SUM(dg.daily_likes), 0) * 1.0 / NULLIF(DATEDIFF(day, MIN(nw.start_date), CURRENT_DATE), 0), 2) AS avg_likes_per_day,
        ROUND(COALESCE(SUM(dg.daily_comments), 0) * 1.0 / NULLIF(DATEDIFF(day, MIN(nw.start_date), CURRENT_DATE), 0), 2) AS avg_comments_per_day
    FROM new_webtoons nw
    LEFT JOIN daily_growth dg ON nw.title_id = dg.title_id  -- LEFT JOIN 유지 (데이터 없는 경우 포함)
    GROUP BY nw.title_id, nw.title, nw.author, nw.platform
)
-- 성장률이 높은 신규 웹툰 TOP 10 선정
SELECT *
FROM growth_summary
ORDER BY avg_likes_per_day DESC
LIMIT 10
