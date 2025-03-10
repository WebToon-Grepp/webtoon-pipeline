WITH episodes_engagement AS (
    -- 웹툰별 총 좋아요(likes)와 댓글(comments) 수를 계산
    SELECT
        platform,
        title_id,
        SUM(likes) AS total_likes,
        SUM(comments) AS total_comments
    FROM raw_data.episodes
    GROUP BY platform, title_id   
)

-- 장르별 총 좋아요(likes) 및 댓글(comments) 수 계산
SELECT 
    g.genre_name,
    SUM(ee.total_likes) AS likes,
    SUM(ee.total_comments) AS comments 
FROM episodes_engagement AS ee
RIGHT JOIN raw_data.genres AS g
ON g.title_id = ee.title_id
GROUP BY g.genre_name
ORDER BY likes DESC
