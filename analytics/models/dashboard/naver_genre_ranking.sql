WITH episodes_engagement AS (
    -- 웹툰별 총 좋아요(likes), 댓글(comments), 조회수(views) 합산
    SELECT
        e.platform,
        e.title_id,
        SUM(e.likes) AS total_likes,
        SUM(e.comments) AS total_comments,
        SUM(t.views) AS total_views,
        SUM(e.likes) + SUM(e.comments) + SUM(t.views) AS popularity_score  -- 인기 지수 정의
    FROM raw_data.episodes e
    JOIN raw_data.titles t ON e.title_id = t.id  -- 조회수 정보 결합
    WHERE e.platform = 'naver'  -- 네이버만 필터링
    GROUP BY e.platform, e.title_id
),
genre_rank AS (
    -- 네이버 장르의 총 인기 지수를 계산하고 랭킹 부여
    SELECT 
        ee.platform,
        g.genre_name,
        SUM(ee.total_likes) AS total_likes,
        SUM(ee.total_comments) AS total_comments,
        SUM(ee.total_views) AS total_views,
        SUM(ee.popularity_score) AS popularity_score, -- 최종 인기 지수 합산
        RANK() OVER (ORDER BY SUM(ee.popularity_score) DESC) AS rank
    FROM episodes_engagement ee
    JOIN raw_data.genres g ON g.title_id = ee.title_id
    GROUP BY ee.platform, g.genre_name
)
-- 네이버 인기 장르 TOP 10
SELECT *
FROM genre_rank
WHERE rank <= 10
ORDER BY rank