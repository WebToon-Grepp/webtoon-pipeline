WITH episodes AS (

    SELECT 
        *, 
       ROW_NUMBER() OVER (PARTITION BY platform, title_id ORDER BY updated_date DESC, id DESC) AS episode_no
    FROM raw_data.episodes

)

SELECT 
    *
FROM episodes
WHERE episode_no < 100

