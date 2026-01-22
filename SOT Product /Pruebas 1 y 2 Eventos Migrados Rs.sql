SELECT 
    time_month,
    event_name,
    COUNT(*) AS volumen_total,
    COUNT(DISTINCT id_company) AS empresas_unicas
FROM (
    SELECT DISTINCT
    id_company
    ,CASE WHEN event_name = 'ac-invoice-created' THEN 'ac-invoice-submitted' ELSE event_name END AS event_name
    ,event_time
    ,DATE_TRUNC('month', event_time) AS time_month
    FROM (
        SELECT DISTINCT
        id_company
        ,event_name
        ,event_time
        FROM db_amplitude_events.amplitude_ac_events
        WHERE event_time >= '2024-01-01'

        UNION ALL

        SELECT DISTINCT
        id_company
        ,event_name
        ,event_time
        FROM db_amplitude_events.amplitude_attribution
        WHERE event_time >= '2024-01-01'
    ) AS a
) AS a
WHERE event_name IN (
    'ac-account-information-filled'
    ,'ac-first-step-managed'
    ,'ac-onb-user-information-filled'
    ,'ac-onboarding-finished'
    ,'ac-onboarding-started'
    ,'ac-role-selected'
    ,'ac-item-created'
    ,'eco-wizard-finished'
    ,'ac-invoice-submitted'
    ,'ac-bill-created'
    ,'ac-report-generated'
    ,'ac-transaction-in-created'
    ,'ac-support-document-created'
    ,'ac-report-visited'
  )
GROUP BY 1, 2
ORDER BY 2, 1
