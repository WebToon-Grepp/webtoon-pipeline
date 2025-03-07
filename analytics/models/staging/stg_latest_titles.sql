WITH titles AS (

    SELECT 
        *, 
       ROW_NUMBER() OVER (PARTITION BY id, release_day ORDER BY views DESC) AS title_no
    FROM raw_data.titles

)

SELECT 
    *
FROM titles
WHERE title_no = 1

