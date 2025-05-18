SELECT 
    da.artist_name,
    ROUND(AVG(ftt.average_voting)::numeric, 2) AS avg_rating,
    COUNT(ftt.key_title) AS title_count
FROM 
    fact_track_title ftt
JOIN 
    dim_artist da ON ftt.key_artist = da.key_artist
GROUP BY 
    da.artist_name
HAVING 
    COUNT(ftt.key_title) >= 2
	AND AVG(ftt.average_voting) >= 7.0
ORDER BY 
    avg_rating DESC;
