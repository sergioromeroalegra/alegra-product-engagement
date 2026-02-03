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

,alegra_companies AS (
    SELECT id_company
    ,company_employees
    ,company_phone
    FROM (
        SELECT
            id AS id_company
            ,employeesnumber AS company_employees
            ,phone AS company_phone
            ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) as rn
        FROM alegra.companies
    )
    WHERE rn = 1
)

,revenue_tiers AS (
    SELECT
    id_company
    ,company_onb_revenue_tiers
    FROM (
        SELECT
        company_id AS id_company
        ,CASE WHEN mql_tier = 'Lite' THEN 'Tier 1 Revenue'
            WHEN mql_tier = 'Tier 2 Core' THEN 'Tier 2 Revenue'
            WHEN mql_tier = 'Tier 3 Core' THEN 'Tier 3 Revenue'
            ELSE 'Sin clasificacion'
            END AS company_onb_revenue_tiers
        ,ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY company_id) AS rn
        FROM db_hubspot.mql_the_blip
    )
    WHERE rn = 1
)

,register_device_category AS (
    SELECT
    id_company
    ,sign_up_device_category
    FROM (
        SELECT
        idcompany AS id_company
        ,device_type AS sign_up_device_category
        ,ROW_NUMBER() OVER (PARTITION BY idcompany ORDER BY datetime) AS rank_device_type
        FROM db_monolitico.mv_company_register_agents
    ) AS a
    WHERE rank_device_type = 1
)

,demo_plan AS (
    SELECT *
    FROM (
        SELECT
        a.id_product
        ,a.id_plan
        ,a.plan_name
        ,a.internal_name
        ,b.id_company
        ,b.plan_start_date AS demo_plan_start_date
        ,b.plan_end_date AS demo_plan_end_date
        ,ROW_NUMBER () OVER (PARTITION BY b.id_company, a.id_product ORDER BY b.plan_start_date) AS rn
        FROM (
            SELECT
            id_product
            ,id_plan
            ,plan_name
            ,internal_name
            FROM dwh_dimensions.dim_plan
            WHERE id_product = 1
                AND plan_name ILIKE '%demo%'
        ) AS a

        LEFT JOIN (
            SELECT
            idcompany AS id_company
            ,idplan AS id_plan
            ,datestart AS plan_start_date
            ,dateend AS plan_end_date
            FROM data_table_bi.db_membership.subscriptions
        ) AS b
        ON a.id_plan = b.id_plan
    )
    WHERE rn = 1
)

,logos AS (
    SELECT
    id_company
    ,id_product
    ,date AS logo_conversion_date
    ,event_logo
    ,event_product
    FROM dwh_facts.fact_customers_mrr
    WHERE id_product = 1
        AND event_product = 'NEW' -- Es su primer orden de pago de ese producto
)

,sales_actions AS (
    SELECT
    id_company
    ,fecha
    ,gestion
    ,detalle_gestion
    ,contactable
    -- Aplicamos tu diccionario manual aquí:
    ,CASE 
        -- 1. INCONTACTABLE
        WHEN detalle_gestion ~* 'cuelga|incorrecto|sin datos|errado|no comunicaci.n|incontactable'
                THEN 'Incontactable'
        
        -- 2. NO PERFIL / JUNK
        WHEN detalle_gestion ~* 'duplicada|estudiante|competencia|lista negra|prueba' 
                THEN 'No Perfil (Junk)'
        
        -- 3. CONTACTADO
        ELSE 'Contactable'
        END AS contactabilidad_adj
    ,ROW_NUMBER() OVER (PARTITION BY id_company ORDER BY fecha DESC) as rn
    FROM bi_sales.sales_actions
    WHERE gestion IN ('Onboarding terminado','Lead descalificado','Leads contactables')
        --AND id_company = 1539229
)
--SQL: ´bi_sales.sql´


/*
SELECT
gestion
,detalle_gestion
--,COUNT(*)
FROM -*/
SELECT
a.sign_up_type
,a.sign_up_date
,a.id_company
,a.country
,a.product_name
,a.id_product
,b.acquisition_channel_name
,bb.sign_up_device_category
,c.event_date_onb_started
,c.event_date_role_selected
,d.company_profile
,c.event_date_user_info_filled
,e.user_company_position
,c.event_date_account_info_filled
,f.company_sector
,g.company_employees
,g.company_phone
,h.company_onb_revenue_tiers
,c.event_date_onb_finished
,i.id_plan
,i.plan_name
,i.internal_name
,i.demo_plan_start_date
,i.demo_plan_end_date
,CASE WHEN j.logo_conversion_date < i.demo_plan_end_date THEN j.logo_conversion_date ElSE i.demo_plan_end_date END AS demo_plan_end_date_adj
,DATEDIFF('days',i.demo_plan_start_date,demo_plan_end_date_adj) AS demo_days
,j.logo_conversion_date
,j.event_logo
,j.event_product
,k.fecha
,k.gestion
,k.detalle_gestion
,k.contactable
,k.contactabilidad_adj
FROM fact_sign_ups AS a

LEFT JOIN fact_acquisition_channel AS b
    ON a.id_company = b.id_company
    AND a.id_product = b.id_product

LEFT JOIN register_device_category AS bb
    ON a.id_company = bb.id_company

LEFT JOIN pivot_amplitude_onboarding AS c
    ON a.id_company = c.id_company
    AND a.id_product = c.id_product

LEFT JOIN dim_subscribers AS d
    ON a.id_company = d.id_company
    AND c.event_date_role_selected IS NOT NULL

LEFT JOIN alegra_users AS e
    ON a.id_company = e.id_company
    AND c.event_date_user_info_filled IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN dim_subscribers AS f
    ON a.id_company = f.id_company
    AND c.event_date_account_info_filled IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN alegra_companies AS g
    ON a.id_company = g.id_company
    AND c.event_date_account_info_filled IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN revenue_tiers AS h
    ON a.id_company = h.id_company
    AND c.event_date_account_info_filled IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN demo_plan AS i
    ON a.id_company = i.id_company
    AND a.id_product = i.id_product
    AND c.event_date_onb_finished IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN logos AS j
    ON a.id_company = j.id_company
    AND a.id_product = j.id_product
    AND c.event_date_onb_finished IS NOT NULL
    AND d.company_profile = 'entrepreneur'

LEFT JOIN sales_actions AS k
    ON a.id_company = k.id_company
    AND c.event_date_onb_finished IS NOT NULL
    AND d.company_profile = 'entrepreneur'
    AND k.fecha >= i.demo_plan_start_date
    AND k.fecha <= CASE WHEN j.logo_conversion_date < i.demo_plan_end_date THEN j.logo_conversion_date ElSE i.demo_plan_end_date END

--WHERE a.id_company IN (2180340,2171702,2172572,2182902,2183695)
/*
)
GROUP BY 1,2
HAVING COUNT(*) > 1*/
