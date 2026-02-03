WITH fact_sign_ups AS (
    SELECT
    event_type
    ,CASE WHEN event_type = 'LOGO' THEN 'New Alegra User' ELSE 'Current Alegra User' END AS sign_up_type
    ,id_date_registration_alegra
    ,TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') AS sign_up_date
    ,id_company
    ,app_version
    ,app_version AS country
    ,product_name
    ,id_product
    ,segment_type_onb
    FROM dwh_facts.fact_sign_ups
    WHERE event_type = 'LOGO'
    AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-12-01' AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') <= '2025-12-31'
    AND app_version IN ('colombia')
    AND id_product = 1
)

,fact_acquisition_channel AS (
    SELECT
    id_company
    ,id_product
    ,app_version
    ,app_version AS country
    ,initial_utm_channel AS acquisition_channel_name
    FROM bi_growth.fact_attribution
    WHERE id_product = 1
        AND app_version = 'colombia'
        --AND id_company = 1543189
    GROUP BY 1,2,3,5
)

,amplitude_onboarding AS (
    SELECT
    id_company
    ,event_time
    ,event_time::date AS event_date
    ,event_name
    ,id_product
    FROM db_amplitude_events.amplitude_attribution
    WHERE event_name IN ('ac-onboarding-started','ac-role-selected','ac-account-information-filled','ac-onb-user-information-filled','ac-onboarding-finished')
        AND event_time >= '2025-11-30'
)

,pivot_amplitude_onboarding AS (
    SELECT
    id_company,
    id_product,
    -- Usamos MIN para capturar la PRIMERA vez que el usuario hizo esa acción
    MIN(CASE WHEN event_name = 'ac-onboarding-started' THEN event_date END) AS event_date_onb_started,
    MIN(CASE WHEN event_name = 'ac-role-selected' THEN event_date END) AS event_date_role_selected,
    MIN(CASE WHEN event_name = 'ac-onb-user-information-filled' THEN event_date END) AS event_date_user_info_filled,
    MIN(CASE WHEN event_name = 'ac-account-information-filled' THEN event_date END) AS event_date_account_info_filled,
    MIN(CASE WHEN event_name = 'ac-onboarding-finished' THEN event_date END) AS event_date_onb_finished
    FROM amplitude_onboarding
    GROUP BY 1, 2
)

,dim_subscribers AS (
    SELECT
    idcompany AS id_company
    ,sector AS company_sector
    ,profile AS company_profile
    ,segment_type_sales
    FROM dwh_dimensions.dim_subscribers
)

,alegra_users AS (
    SELECT 
    idcompany AS id_company
    ,JSON_EXTRACT_PATH_TEXT(metadata, 'position') AS user_company_position
    FROM alegra.users
    WHERE idlocal = 1
) 

SELECT
a.sign_up_type
,a.sign_up_date
,a.id_company
,a.country
,a.product_name
,a.id_product
,b.acquisition_channel_name
,c.event_date_onb_started
,c.event_date_role_selected
,d.company_profile
,c.event_date_user_info_filled
,e.user_company_position
,c.event_date_account_info_filled
,c.event_date_onb_finished
FROM fact_sign_ups AS a

LEFT JOIN fact_acquisition_channel AS b
    ON a.id_company = b.id_company
    AND a.id_product = b.id_product

LEFT JOIN pivot_amplitude_onboarding AS c
    ON a.id_company = c.id_company
    AND a.id_product = c.id_product

LEFT JOIN dim_subscribers AS d
    ON a.id_company = d.id_company
    AND c.event_date_role_selected IS NOT NULL

LEFT JOIN alegra_users AS e
    ON a.id_company = e.id_company
    AND c.event_date_user_info_filled IS NOT NULL
    AND CASE WHEN d.company_profile = 'entrepreneur' THEN 1 ELSE 0 END = 1
