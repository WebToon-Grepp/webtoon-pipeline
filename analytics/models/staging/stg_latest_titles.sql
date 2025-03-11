WITH titles AS (

    SELECT 
        *, 
       TO_DATE(year || '-' || month || '-' || day, 'YYYY-MM-DD') AS data_date,
       ROW_NUMBER() OVER (PARTITION BY platform, id, release_day ORDER BY data_date DESC) AS title_no
    FROM raw_data.titles

)

SELECT 
    platform, 
    id, 
    title, 
    author, 
    CASE 
        WHEN views IS NULL THEN FLOOR(1 + (RAND() * 1000000))
        WHEN views = 0 THEN FLOOR(1 + (RAND() * 1000000))
        ELSE views 
    END AS views, 
    image_url,
    COALESCE(release_day, 7) AS release_day, 
    is_completed 
FROM titles
WHERE title_no = 1

