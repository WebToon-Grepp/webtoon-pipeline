WITH webtoon_engagement AS (
    -- 각 작품(웹툰)별 좋아요, 댓글, 조회수 합산하여 인기 점수 계산
    SELECT
        t.platform,
        t.title,  -- 웹툰 제목
        SUM(e.likes) AS total_likes,
        SUM(e.comments) AS total_comments,
        SUM(t.views) AS total_views,
        -- 인기도 점수 (좋아요 + 댓글 + 조회수 합산)
        SUM(e.likes) + SUM(e.comments) + SUM(t.views) AS popularity_score
    FROM raw_data.episodes e
    JOIN raw_data.titles t
    ON e.title_id = t.id
    GROUP BY t.platform, t.title
),
ranked_webtoons AS (
    -- 플랫폼별 인기 작품 TOP 10 선정
    SELECT 
        platform,
        title,
        total_likes,
        total_comments,
        total_views,
        popularity_score,
        RANK() OVER (PARTITION BY platform ORDER BY popularity_score DESC) AS rank
    FROM webtoon_engagement
)
-- 네이버와 카카오 각각 TOP 10 작품만 선택
SELECT * 
FROM ranked_webtoons
WHERE rank <= 10
ORDER BY platform, rank
