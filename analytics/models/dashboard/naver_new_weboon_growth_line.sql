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
    WHERE platform = 'naver'
    AND TO_DATE(year::TEXT || '-' || LPAD(month::TEXT, 2, '0') || '-' || LPAD(day::TEXT, 2, '0'), 'YYYY-MM-DD')
        >= DATEADD(day, -30, CURRENT_DATE)  -- 최근 30일 내 웹툰만 포함
),
daily_growth AS (
    -- 신규 웹툰의 일별 좋아요 & 댓글 증가량 계산 (최근 30일)
    SELECT
        e.title_id,
        e.updated_date AS growth_date,
        COALESCE(SUM(e.likes), 0) AS daily_likes,  
        COALESCE(SUM(e.comments), 0) AS daily_comments  
    FROM raw_data.episodes e
    INNER JOIN new_webtoons nw ON e.title_id = nw.title_id  
    WHERE e.platform = 'naver'
    AND e.updated_date >= DATEADD(day, -30, CURRENT_DATE)  -- 최근 30일치 데이터만 포함
    GROUP BY e.title_id, e.updated_date
),
growth_summary AS (
    -- 웹툰별 총 좋아요 및 댓글 증가량, 일평균 좋아요 증가량 계산
    SELECT
        nw.title_id,
        nw.title,
        nw.author,
        nw.platform,
        MIN(nw.start_date) AS start_date,
        COALESCE(SUM(dg.daily_likes), 0) AS total_likes,  
        COALESCE(SUM(dg.daily_comments), 0) AS total_comments,  
        ROUND(COALESCE(SUM(dg.daily_likes), 0) * 1.0 / NULLIF(DATEDIFF(day, MIN(nw.start_date), CURRENT_DATE), 0), 2) AS avg_likes_per_day,
        ROUND(COALESCE(SUM(dg.daily_comments), 0) * 1.0 / NULLIF(DATEDIFF(day, MIN(nw.start_date), CURRENT_DATE), 0), 2) AS avg_comments_per_day
    FROM new_webtoons nw
    LEFT JOIN daily_growth dg ON nw.title_id = dg.title_id  
    GROUP BY nw.title_id, nw.title, nw.author, nw.platform
),
top_5_webtoons AS (
    -- 성장률이 높은 신규 웹툰 TOP 10 선정
    SELECT title_id
    FROM growth_summary
    ORDER BY avg_likes_per_day DESC
    LIMIT 10
)
-- 최종: 최근 30일 동안의 성장 데이터를 가져오기
SELECT 
    dg.growth_date,
    dg.title_id,
    nw.title,
    nw.author,
    dg.daily_likes,
    dg.daily_comments
FROM daily_growth dg
INNER JOIN top_5_webtoons t5 ON dg.title_id = t5.title_id
INNER JOIN new_webtoons nw ON dg.title_id = nw.title_id
ORDER BY dg.growth_date, dg.title_id
