SELECT 
    dp.profession,
    COUNT(*) AS high_rated_count
FROM 
    fact_track_title ftt
JOIN 
    bridge_profession bp ON ftt.key_artist = bp.key_artist
JOIN 
    dim_profession dp ON bp.key_profession = dp.key_profession
WHERE 
    ftt.average_voting >= 8.0
GROUP BY 
    dp.profession
ORDER BY 
    high_rated_count DESC;
