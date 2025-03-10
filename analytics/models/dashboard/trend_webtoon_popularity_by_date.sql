SELECT 
    updated_date,
    SUM(comments) AS total_comments,
    SUM(likes) AS total_likes
FROM raw_data.episodes
GROUP BY updated_date
ORDER BY total_likes DESC
