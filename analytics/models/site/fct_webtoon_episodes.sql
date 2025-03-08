WITH episodes AS (

    SELECT 
        *
    FROM 
        {{ ref('stg_latest_episodes') }}

)

SELECT DISTINCT 
    platform,
    title_id,
    id,
    title,
    likes,
    comments,
    image_url,
    updated_date
FROM episodes

