WITH join_titles_genres AS (

    SELECT 
        tt.platform, 
        tt.id, 
        tt.title, 
        tt.author, 
        tt.image_url, 
        tt.views, 
        tt.likes, 
        tt.comments, 
        tt.release_day, 
        tt.is_completed, 
        COALESCE(g.genre_name, '완결') AS genre_name
    FROM raw_data.genres g
    FULL JOIN {{ ref('stg_total_titles') }} tt
        ON g.title_id = tt.id 
        AND g.platform = tt.platform 
)

SELECT DISTINCT 
    *
FROM join_titles_genres