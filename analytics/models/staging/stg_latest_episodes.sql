WITH episodes AS (

    SELECT 
        *, 
       TO_DATE(year || '-' || month || '-' || day, 'YYYY-MM-DD') AS data_date,
       ROW_NUMBER() OVER (PARTITION BY platform, title_id, id ORDER BY data_date DESC, id DESC) AS episode_no
    FROM raw_data.episodes

)

SELECT 
    platform,
    title_id,
    id,
    title,
    COALESCE(likes, FLOOR(1 + (RAND() * 3000))) AS likes,
    COALESCE(comments, FLOOR(1 + (RAND() * 100))) AS comments,
    image_url,
    COALESCE(updated_date, 
        (CURRENT_DATE - INTERVAL '1 day' * FLOOR(RANDOM() * 1825))::DATE
    ) AS updated_date
FROM episodes
WHERE episode_no = 1

