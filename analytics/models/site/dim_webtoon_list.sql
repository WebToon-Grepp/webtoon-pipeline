WITH join_titles_episodes AS (

    SELECT 
        t.platform, 
        t.id, 
        t.title, 
        t.author, 
        t.image_url, 
        t.views, 
        SUM(e.likes) AS likes, 
        SUM(e.comments) AS comments, 
        COALESCE(t.release_day, 7) AS release_day, 
        t.is_completed 
    FROM {{ ref('stg_latest_episodes') }} e 
    INNER JOIN {{ ref('stg_latest_titles') }} t 
        ON e.title_id = t.id 
        AND e.platform = t.platform 
    GROUP BY 
        t.platform, 
        t.id, 
        t.title, 
        t.author, 
        t.views, 
        t.image_url, 
        t.release_day, 
        t.is_completed 
        
    -- 추후 진행
    -- SELECT 
    --     t.platform, 
    --     t.id, 
    --     t.title, 
    --     t.author, 
    --     t.image_url, 
    --     t.views, 
    --     SUM(e.likes) AS likes, 
    --     SUM(e.comments) AS comments, 
    --     COALESCE(t.release_day, 7) AS release_day, 
    --     t.is_completed 
    -- FROM {{ ref('stg_latest_episodes') }} e 
    -- INNER JOIN {{ ref('stg_latest_titles') }} t 
    --     ON e.title_id = t.id 
    --     AND e.platform = t.platform 
    -- GROUP BY 
    --     t.platform, 
    --     t.id, 
    --     t.title, 
    --     t.author, 
    --     t.views, 
    --     t.image_url, 
    --     t.release_day, 
    --     t.is_completed 
)

SELECT DISTINCT 
    *
FROM join_titles_episodes