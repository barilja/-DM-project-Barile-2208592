SELECT 
    da.artist_name,
    COUNT(DISTINCT ftt.key_title) AS high_rated_title_count
FROM 
    fact_track_title ftt
JOIN 
    dim_artist da ON ftt.key_artist = da.key_artist
WHERE 
    ftt.average_voting >= 8.0
GROUP BY 
    da.artist_name
ORDER BY 
    high_rated_title_count DESC
LIMIT 15;
