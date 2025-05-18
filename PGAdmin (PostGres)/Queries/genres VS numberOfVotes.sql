SELECT 
    dg.genre,
    COUNT(*) AS title_count,
    SUM(ftt.number_of_votes) AS total_votes
FROM 
    fact_track_title ftt
JOIN 
    bridge_genre bg ON ftt.key_title = bg.key_title
JOIN 
    dim_genre dg ON bg.key_genre = dg.key_genre
GROUP BY 
    dg.genre
ORDER BY 
    total_votes DESC;
