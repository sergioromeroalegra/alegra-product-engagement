WITH sign_ups AS (
    SELECT
        id_company
        ,id_product
        ,TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') AS id_product_sign_up_date
        ,app_version AS country
        ,segment_type_onb
    FROM dwh_facts.fact_sign_ups
    WHERE
        id_product = 1
        AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-09-01'
        AND app_version = 'colombia'
        AND event_type = 'LOGO' --usuarios nuevos de Alegra
)

,dim_subscribers AS (
    SELECT
        idcompany AS id_company
        ,segment_type_sales
        ,sector AS company_sector
    FROM dwh_dimensions.dim_subscribers
    WHERE 
        appversion = 'colombia'
        AND profile = 'entrepreneur'
)

,entrepreneurs AS (
    SELECT
        a.id_company
        ,a.id_product
        ,a.id_product_sign_up_date
        ,a.country
        ,a.segment_type_onb
        ,b.segment_type_sales
        ,b.company_sector
    FROM sign_ups AS a

    JOIN dim_subscribers AS b ON a.id_company = b.id_company

    GROUP BY 1,2,3,4,5,6,7
)

,acquisition_channel AS (
    SELECT 
        id_company
        ,id_product_acquisition_channel_name
        ,id_product_acquisition_channel_group
    FROM (
        SELECT 
            id_company, 
            initial_utm_channel AS id_product_acquisition_channel_name,
            CASE 
                WHEN initial_utm_channel IN ('Paid Media (SEM + Social)', 'Influencers') THEN 'Paid'
                WHEN initial_utm_channel IN ('SEO / Organic Traffic', 'Direct Traffic', 'Referrals', 'Social Media (Organico)') THEN 'Orgánico'
                ELSE 'Otros' 
            END AS id_product_acquisition_channel_group,
            ROW_NUMBER() OVER (PARTITION BY id_company ORDER BY initial_utm_channel DESC) as rn
        FROM bi_growth.fact_attribution
        WHERE 
            id_product = 1 
            AND app_version = 'colombia'
    ) WHERE rn = 1
)

,sign_up_device AS (
    SELECT id_company, sign_up_device_category
    FROM (
        SELECT 
            idcompany AS id_company, 
            device_type AS sign_up_device_category,
            ROW_NUMBER() OVER (PARTITION BY idcompany ORDER BY datetime) AS rn
        FROM db_monolitico.mv_company_register_agents
    ) WHERE rn = 1
)

,onb_finished AS (
    SELECT
        id_company
        ,id_product_onb_finish_date
        ,id_product_onb_finish_device_category
    FROM (
        SELECT 
            id_company
            ,device_category
            ,CASE 
                WHEN device_category = 'Desktop' THEN 'pc'
                WHEN device_category = 'Mobile' THEN 'mobile'
                ELSE 'other'
            END AS id_product_onb_finish_device_category
            ,event_time
            ,event_time::date AS id_product_onb_finish_date
            ,ROW_NUMBER () OVER (PARTITION BY id_company ORDER BY event_time) AS rn
        FROM db_amplitude_events.amplitude_mql_events 
        WHERE event_name = 'ac-onboarding-finished-backend'
        AND event_timestamp >= '2025-09-01'
        --AND id_company IN (2088657,2093250,2116892,2158912,2187397)
    )
    WHERE rn = 1
)

,pqls AS (
    SELECT 
        id_company, 
        MIN(event_time)::date AS id_product_first_pql_date
    FROM db_amplitude_events.amplitude_ac_events
    WHERE event_time >= '2025-09-01'
      AND event_name IN ('ac-invoice-created', 'ac-invoice-submitted')
    GROUP BY 1
)

,dim_plan AS (
    SELECT 
    id_plan
    ,internal_name AS demo_internal_name
    FROM dwh_dimensions.dim_plan 
    WHERE 
        id_product = 1 
        AND plan_name ILIKE '%demo%'
)

,subscription AS (
    SELECT
        idcompany AS id_company
        ,idplan AS id_plan
        ,datestart AS plan_start_date
        ,dateend AS plan_end_date
    FROM data_table_bi.db_membership.subscriptions
)

,demo_period AS (
    SELECT *
    FROM (
        SELECT 
            b.id_company
            ,a.demo_internal_name
            ,b.plan_start_date AS id_product_demo_start_date
            ,b.plan_end_date AS id_product_demo_end_date
            ,ROW_NUMBER() OVER (PARTITION BY b.id_company ORDER BY b.plan_start_date) AS rn
        FROM dim_plan AS a

        JOIN subscription b ON a.id_plan = b.id_plan
    )
    WHERE rn = 1
)

,logos AS (
    SELECT 
        id_company, 
        date AS id_product_purchase_date
    FROM dwh_facts.fact_customers_mrr
    WHERE id_product = 1 
        AND event_product = 'NEW'
        --AND id_company = 2040761
)

,demo_period_adj AS (
    SELECT 
        a.id_company
        ,b.id_product_demo_start_date
        ,DATEADD('day',15,b.id_product_demo_start_date)::date AS manual_end_demo
        ,b.id_product_demo_end_date
        ,c.id_product_purchase_date
        ,LEAST(COALESCE(c.id_product_purchase_date, '2099-12-31'), COALESCE(b.id_product_demo_end_date,DATEADD('day',15,b.id_product_demo_start_date)::date)) AS id_product_demo_end_date_adj
    FROM entrepreneurs AS a

    LEFT JOIN demo_period AS b ON a.id_company = b.id_company
    LEFT JOIN logos AS c ON a.id_company = c.id_company
)

--,base AS (
    SELECT
        a.country
        ,a.id_company
        ,a.id_product
        ,a.id_product_sign_up_date
        ,b.id_product_acquisition_channel_name
        ,b.id_product_acquisition_channel_group
        ,e.sign_up_device_category
        ,d.id_product_onb_finish_device_category
        ,COALESCE(
            d.id_product_onb_finish_date, -- Prioridad 1: Frontend
            h.id_product_first_pql_date,  -- Prioridad 2: PQL
            i.id_product_purchase_date    -- Prioridad 3: Compra
        ) AS id_product_onb_finish_date_adj
        ,h.id_product_first_pql_date
        ,i.id_product_purchase_date
        ,j.id_product_demo_start_date
        ,j.id_product_demo_end_date_adj
        -- La Variable ICP (Concatenación Simple)
        ,b.id_product_acquisition_channel_group || ' + ' || e.sign_up_device_category AS icp_profile
    FROM entrepreneurs AS a
    LEFT JOIN acquisition_channel AS b ON a.id_company = b.id_company
    LEFT JOIN sign_up_device AS e ON a.id_company = e.id_company
    LEFT JOIN onb_finished AS d ON a.id_company = d.id_company
    LEFT JOIN pqls AS h ON a.id_company = h.id_company
    LEFT JOIN logos AS i ON a.id_company = i.id_company
    LEFT JOIN demo_period_adj AS j ON a.id_company = j.id_company

    WHERE 
        COALESCE(
            d.id_product_onb_finish_date, -- Prioridad 1: Frontend
            h.id_product_first_pql_date,  -- Prioridad 2: PQL
            i.id_product_purchase_date    -- Prioridad 3: Compra
        ) IS NOT NULL
        AND b.id_product_acquisition_channel_group IN ('Paid','Orgánico')
        AND e.sign_up_device_category IN ('pc','mobile')
        AND d.id_product_onb_finish_device_category = 'other';
