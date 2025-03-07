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
    COALESCE(likes, 0) AS likes,
    COALESCE(comments, 0) AS comments,
    image_url,
    COALESCE(updated_date, '2025-02-26') AS updated_date
FROM episodes

-- 추후 진행
-- SELECT DISTINCT 
--     platform,
--     title_id,
--     id,
--     title,
--     likes,
--     comments,
--     image_url,
--     updated_date
-- FROM episodes

