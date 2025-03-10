WITH webtoon_popularity_by_day AS (
    -- 웹툰별 총 좋아요(likes)와 댓글(comments) 수를 계산
    SELECT 
        title_id,
        SUM(comments) AS total_comments,
        SUM(likes) AS total_likes
    FROM raw_data.episodes
    GROUP BY title_id	
)

-- 요일별 웹툰 조회수, 좋아요, 댓글 수를 집계
SELECT
    rt.release_day,
    SUM(rt.views) AS total_views,
    SUM(day.total_likes) AS total_likes,
    SUM(day.total_comments) AS total_comments
FROM raw_data.titles AS rt
LEFT JOIN webtoon_popularity_by_day AS day
ON rt.id = day.title_id
GROUP BY rt.release_day
