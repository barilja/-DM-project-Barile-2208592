SELECT 
    dt.track_name,
    da.artist_name,
    dt.release_date AS track_release_date,
    dti.title AS title_name,
    dti.start_year AS title_start_year,
    ftt.popularity,
    ftt.average_voting
FROM 
    fact_track_title ftt
JOIN 
    dim_track dt ON ftt.key_track = dt.key_track
JOIN 
    dim_title dti ON ftt.key_title = dti.key_title
JOIN 
    dim_artist da ON ftt.key_artist = da.key_artist
WHERE 
    ftt.popularity >= 85
ORDER BY 
    ftt.average_voting DESC
LIMIT 50;
