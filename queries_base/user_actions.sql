SELECT 
id_company
,CAST(event_time AS DATE) AS event_date
,REPLACE(event_name, 'app', 'ac') AS event_name
,event_time
,'Amplitude' AS event_source
FROM db_amplitude_events.amplitude_attribution
WHERE id_product = 1
UNION
SELECT
id_company
,CAST(event_time AS DATE) AS event_date
,REPLACE(event_name, 'app', 'ac') AS event_name
,event_time
,'Amplitude' AS event_source
FROM db_amplitude_events.amplitude_ac_events
WHERE id_product = 1
UNION
SELECT
id_company
,CAST(event_time AS DATE) AS event_date
,REPLACE(event_name, 'app', 'ac') AS event_name
,event_time
,'Amplitude' AS event_source
FROM db_amplitude_events.amplitude_pql_events
WHERE id_product = 1
