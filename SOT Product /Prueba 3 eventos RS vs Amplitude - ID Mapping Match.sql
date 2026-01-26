SELECT DISTINCT
COUNT(a.id_company) AS total_registros_rs --Recodar multiplicar por 96% (tasa de usuario que inician el ONB despues del registro)
,COUNT(b.id_company) AS ids_encontrados_amplitude
FROM (
    SELECT DISTINCT
    id_company
    FROM dwh_facts.fact_sign_ups
    WHERE id_date_registration_alegra >= 20240101
        AND id_product = 1
) AS a
LEFT JOIN (
    SELECT DISTINCT
    a.id_company
    --,CASE WHEN event_name = 'ac-invoice-created' THEN 'ac-invoice-submitted' ELSE event_name END AS event_name
    --,event_time
    --,DATE_TRUNC('month', event_time) AS time_month
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

    WHERE a.event_name IN (
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
) AS b
ON a.id_company = b.id_company
