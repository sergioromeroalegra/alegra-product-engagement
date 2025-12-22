SELECT
id_company
,MIN(event_date) AS onb_finished_date
FROM (
    SELECT 
    id_company
    ,CAST(event_time AS DATE) AS event_date
    FROM db_amplitude_events.amplitude_attribution
    WHERE id_product = 1
        AND event_name = 'ac-onboarding-finished'
    UNION
    SELECT
    id_company
    ,CAST(event_time AS DATE) AS event_date
    FROM db_amplitude_events.amplitude_ac_events
    WHERE id_product = 1
        AND event_name = 'ac-onboarding-finished'
    UNION
    SELECT
    id_company
    ,CAST(event_time AS DATE) AS event_date
    FROM db_amplitude_events.amplitude_pql_events
    WHERE id_product = 1
        AND event_name = 'ac-onboarding-finished'
) AS a
GROUP BY a.id_company
