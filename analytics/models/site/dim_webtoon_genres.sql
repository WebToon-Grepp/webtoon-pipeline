WITH join_webtoon_list_genres AS (

    SELECT 
        wl.platform, 
        wl.id, 
        wl.title, 
        wl.author, 
        wl.image_url, 
        wl.views, 
        wl.likes, 
        wl.comments, 
        g.genre_name
    FROM raw_data.genres g
    LEFT JOIN {{ ref('dim_webtoon_list') }} wl
        ON g.title_id = wl.id 
        AND g.platform = wl.platform 
)

SELECT DISTINCT 
    *
FROM join_webtoon_list_genres