-- Traer base de usuarios a analizar --
SELECT DISTINCT
a.country
,a.id_company
,COALESCE(e.id_company_segment_type_def, b.id_company_segment_type_def, a.id_company_segment_type_def) AS id_company_segment_type_def
,b.id_company_profile
,a.sign_up_date
,c.onb_finished_date
,d.event_name
,d.event_date
,d.event_time
,e.logo_date
,e.id_company_used_discount
,f.last_login_date
FROM (
    -- 1. Registros entre Julio y Noviembre de 2025 --
    SELECT
    app_version AS country
    ,id_company
    --,CAST(DATE_TRUNC('year', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_year
    --,CAST(DATE_TRUNC('month', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_month
    ,TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD') AS sign_up_date
    ,segment_type_def AS id_company_segment_type_def --Segmento Core / Lite
    FROM dwh_facts.fact_sign_ups 
    WHERE app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
        AND event_type = 'LOGO' --Primer Sign Up
        AND id_product = 1 --Alegra Contabilidad--
        AND id_date_registration_alegra BETWEEN  20250701 AND 20251130 -- sin comillas en formato YYYYMMDD
) AS a
-- 2. Traer perfil PyMe
JOIN (
    SELECT
    idcompany AS id_company
    ,profile AS id_company_profile --perfil entrepreneur, accountant, independent, student
    ,COALESCE(segment_type_sales,segment_type_onb) AS id_company_segment_type_def
    FROM dwh_dimensions.dim_subscribers
    WHERE profile = 'entrepreneur'
) AS b
ON a.id_company = b.id_company
-- 3. Traer usuarios que finalizan el ONB
JOIN (
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
) AS c
ON a.id_company = c.id_company
-- 4.Información de uso durante el Demo --
LEFT JOIN (
    SELECT DISTINCT
    id_company
    ,event_name
    ,event_date
    ,event_time
    FROM (
        SELECT 
        id_company
        ,CAST(event_time AS DATE) AS event_date
        ,REPLACE(event_name, 'app', 'ac') AS event_name
        ,event_time
        ,'Amplitude' AS event_source
        FROM db_amplitude_events.amplitude_attribution
        WHERE id_product = 1
            AND event_time BETWEEN '2025-07-01 00:00:00' AND '2025-12-15 23:59:59'
        UNION
        SELECT
        id_company
        ,CAST(event_time AS DATE) AS event_date
        ,REPLACE(event_name, 'app', 'ac') AS event_name
        ,event_time
        ,'Amplitude' AS event_source
        FROM db_amplitude_events.amplitude_ac_events
        WHERE id_product = 1
            AND event_time BETWEEN '2025-07-01 00:00:00' AND '2025-12-15 23:59:59'
        UNION
        SELECT
        id_company
        ,CAST(event_time AS DATE) AS event_date
        ,REPLACE(event_name, 'app', 'ac') AS event_name
        ,event_time
        ,'Amplitude' AS event_source
        FROM db_amplitude_events.amplitude_pql_events
        WHERE id_product = 1
            AND event_time BETWEEN '2025-07-01 00:00:00' AND '2025-12-15 23:59:59'
    ) AS a
    WHERE event_name NOT IN ('ac-account-created','ac-account-information-filled','ac-onboarding-started','ac-onboarding-finished','ac-onb-accountantType-selected','ac-role-selected'
    ,'ac-sector-selected','ac-onb-user-information-filled','eco-subscription-payment-received','ac-first-step-managed')
) AS d
ON a.id_company = d.id_company
AND a.sign_up_date <= d.event_date
AND DATEADD(day,14,a.sign_up_date)::date >= d.event_date
-- 5. Información sobre el pago del usuario --
LEFT JOIN (
    SELECT DISTINCT
    id_company
    ,date AS logo_date
    ,CASE WHEN amount_discount IS NULL OR amount_discount = 0 THEN 'No' ELSE 'Yes' END AS id_company_used_discount
    ,segment_type_def AS id_company_segment_type_def
    FROM dwh_facts.fact_customers_mrr
    WHERE date >= '2025-07-01'
        AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
        AND id_product = 1 
        AND event_logo = 'NEW'
        --AND id_company = '2036363'
) AS e
ON a.id_company = e.id_company

LEFT JOIN (
    SELECT
    idcompany AS id_company
    ,MAX(CAST(datetime AS DATE)) AS last_login_date
    FROM db_monolitico.history
    WHERE resource = 'user'
        AND operation = 'login'
        AND datetime >= '2025-07-01 00:00:00'
        --AND datetime < '2025-11-01 00:00:00'
        --AND idcompany = 1942647
    GROUP BY idcompany
) AS f
ON a.id_company = f.id_company
