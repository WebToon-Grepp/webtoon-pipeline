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
        t.release_day, 
        t.is_completed 
    FROM external.episodes e 
    LEFT JOIN external.titles t 
        ON e.platform = t.platform 
        AND e.title_id = t.id 
    GROUP BY 
        t.platform, 
        t.id, 
        t.title, 
        t.author, 
        t.views, 
        t.image_url, 
        t.release_day, 
        t.is_completed 

)

SELECT DISTINCT *
FROM join_titles_episodes
