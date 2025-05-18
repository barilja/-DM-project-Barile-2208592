SELECT 
    dti.type AS title_type,
    COUNT(DISTINCT ftt.key_track) AS track_count,
    ROUND(AVG(ftt.average_voting)::numeric, 2) AS avg_rating
FROM 
    fact_track_title ftt
JOIN 
    dim_title dti ON ftt.key_title = dti.key_title
GROUP BY 
    dti.type
ORDER BY 
    track_count DESC;
