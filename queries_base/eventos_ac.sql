SELECT 
    id_company
    ,CAST(event_time AS DATE) AS event_date
    ,REPLACE(event_name, 'app', 'ac') AS event_name
    ,event_time
    ,'Amplitude' AS event_source
    ,CASE WHEN REPLACE(event_name, 'app', 'ac')  = 'ac-invoice-created' THEN 'Active' ELSE 'ONB' END AS event_type
FROM db_amplitude_events.amplitude_attribution
WHERE id_product = 1
    AND event_time >= '{fecha_inicio} 00:00:00'
    AND REPLACE(event_name, 'app', 'ac') NOT IN ('ac-account-created','ac-onb-accountantType-selected','eco-subscription-payment-received','ac-sector-selected')

UNION -- Usamos UNION para una de-duplicación básica entre tablas de Amplitude

SELECT
    id_company
    ,CAST(event_time AS DATE) AS event_date
    ,REPLACE(event_name, 'app', 'ac') AS event_name
    ,event_time
    ,'Amplitude' AS event_source
    ,CASE WHEN REPLACE(event_name, 'app', 'ac')  = 'ac-first-step-managed' THEN 'ONB' ELSE 'Active' END AS event_type
FROM db_amplitude_events.amplitude_ac_events
WHERE id_product = 1
    AND event_time >= '{fecha_inicio} 00:00:00'

UNION

SELECT
    id_company
    ,CAST(event_time AS DATE) AS event_date
    ,REPLACE(event_name, 'app', 'ac') AS event_name
    ,event_time
    ,'Amplitude' AS event_source
    ,'Active' AS event_type
FROM db_amplitude_events.amplitude_pql_events
WHERE id_product = 1
    AND event_time >= '{fecha_inicio} 00:00:00'
