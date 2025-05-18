SELECT 
    CASE 
        WHEN ftt.popularity >= 90 THEN 'Very High'
        WHEN ftt.popularity >= 80 THEN 'High'
        WHEN ftt.popularity >= 70 THEN 'Medium'
        ELSE 'Low'
    END AS popularity_bucket,
    ROUND(AVG(ftt.average_voting)::numeric, 2) AS avg_rating,
    COUNT(*) AS track_count
FROM 
    fact_track_title ftt
GROUP BY 
    CASE 
        WHEN ftt.popularity >= 90 THEN 'Very High'
        WHEN ftt.popularity >= 80 THEN 'High'
        WHEN ftt.popularity >= 70 THEN 'Medium'
        ELSE 'Low'
    END
ORDER BY 
    avg_rating DESC;
