WITH base_cohort AS (
    SELECT
    id_company
    ,id_product
    ,product_name
    ,TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') AS sign_up_date
    ,CASE WHEN event_type = 'LOGO' THEN 'New Alegra User' ELSE 'Current Alegra User' END AS sign_up_type
    FROM dwh_facts.fact_sign_ups
    WHERE event_type = 'LOGO'
      AND id_product = 1
      AND app_version = 'colombia'
      AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-09-01'
)

,target_profiles AS (
    SELECT 
    idcompany AS id_company
    ,sector AS company_sector
    ,profile AS company_profile
    ,segment_type_sales
    FROM dwh_dimensions.dim_subscribers
    WHERE profile = 'entrepreneur' -- <--- FILTRO MAESTRO (Early Filtering)
)

-- 3. DATOS FIRMOGRÁFICOS (Corregido con De-duplicación)
,firmographics AS (
    SELECT 
    id_company
    ,company_employees_adj
    ,company_phone
    FROM (
        SELECT 
        c.id AS id_company
        ,CASE 
            WHEN c.employeesnumber IN ('1') THEN '1'
            WHEN c.employeesnumber IN ('1-2', '2-6', '3-6') THEN '2-10'
            WHEN c.employeesnumber IN ('7-15', '16-25', '16-30') THEN '11-30'
            WHEN c.employeesnumber IN ('26-50', 'Más de 30') THEN '31-50'
            WHEN c.employeesnumber = '50+' THEN '51+'
            ELSE NULL 
            END AS company_employees_adj
        ,c.phone AS company_phone
        ,ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.id) as rn
        FROM alegra.companies c
        WHERE c.id IN (SELECT id_company FROM base_cohort) -- Filtro de optimización
    )
    WHERE rn = 1
)

,attribution_data AS (
    SELECT
    id_company
    ,acquisition_channel_name
    ,channel_category
    FROM (
        SELECT
        id_company
        ,initial_utm_channel AS acquisition_channel_name
        ,CASE 
            WHEN initial_utm_channel IN ('Paid Media (SEM + Social)', 'Influencers') THEN 'Paid'
            WHEN initial_utm_channel IN ('SEO / Organic Traffic', 'Direct Traffic', 'Referrals', 'Social Media (Organico)') THEN 'Orgánico'
            WHEN initial_utm_channel IN ('Lead Nurturing', 'Product Marketing', 'Others', 'Eventos', 'Alianzas') THEN 'Baja Escala'
            ELSE 'Otros'
        END AS channel_category
        -- RANKING: Si hay duplicados, elegimos uno arbitrariamente (o por fecha si tuvieras el campo)
        ,ROW_NUMBER() OVER (PARTITION BY id_company ORDER BY initial_utm_channel DESC) as rn
        FROM bi_growth.fact_attribution
        WHERE id_product = 1 
          AND app_version = 'colombia'
    )
    WHERE rn = 1
)

-- 5. DEVICE INFO (Simplificado)
,device_data AS (
    SELECT
    id_company
    ,sign_up_device_category
    FROM (
        SELECT
        idcompany AS id_company
        ,device_type AS sign_up_device_category
        ,ROW_NUMBER() OVER (PARTITION BY idcompany ORDER BY datetime) AS rn
        FROM db_monolitico.mv_company_register_agents
    )
    WHERE rn = 1 -- *Nota: Si tu Redshift es viejo y no soporta QUALIFY, usa el subquery tradicional
)

,onboarding_events AS (
    SELECT
        id_company
        -- Usamos MIN para agarrar la primera fecha si lo hicieron varias veces
        ,MIN(event_time)::date AS date_onb_finished_frontend
    FROM db_amplitude_events.amplitude_attribution
    WHERE event_name = 'ac-onboarding-finished' -- Filtro directo, mucho más rápido que un IN (...)
      AND event_time >= '2025-08-30'
      AND id_product = 1
    GROUP BY 1
)

a AS (
    SELECT *
    FROM (
        SELECT
        id_company
        ,event_name --ac-onboarding-finished-backend
        ,id_product
        ,event_timestamp
        ,event_timestamp::date AS event_date
        ,device_category AS id_product_onboarding_finished_device_cateogory
        ,ROW_NUMBER() OVER (PARTITION BY id_company ORDER BY event_timestamp DESC) AS rn
        FROM db_amplitude_events.amplitude_mql_events 
        WHERE event_name = 'ac-onboarding-finished-backend'
        AND id_company = 660298
    )
    WHERE rn = 1
)

,dim_plan AS (
    SELECT
    id_product
    ,id_plan
    ,plan_name
    ,internal_name
    FROM dwh_dimensions.dim_plan
    WHERE id_product = 1 AND plan_name ILIKE '%demo%'
)

,subscriptions AS (
    SELECT
    idcompany AS id_company
    ,idplan AS id_plan
    ,datestart AS plan_start_date
    ,dateend AS plan_end_date
    FROM data_table_bi.db_membership.subscriptions
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
        FROM dim_plan AS a

        LEFT JOIN subscriptions AS b
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
/*
,time_window_logic AS (
    SELECT 
        b.id_company
        ,sub.datestart AS demo_start_date
        ,sub.dateend AS demo_end_date
        ,l.date AS purchase_date
        -- Lógica: La ventana de venta termina cuando compra O cuando se acaba el demo
        ,LEAST(COALESCE(l.date, '2099-12-31'), sub.dateend) AS effective_sales_window_end
    FROM base_cohort b
    -- Join con Planes (Demo)
    INNER JOIN dwh_dimensions.dim_plan p ON p.id_product = 1 AND p.plan_name ILIKE '%demo%'
    INNER JOIN data_table_bi.db_membership.subscriptions sub 
        ON sub.idcompany = b.id_company AND sub.idplan = p.id_plan
    -- Join con Compras (Logos)
    LEFT JOIN dwh_facts.fact_customers_mrr l 
        ON l.id_company = b.id_company AND l.id_product = 1 AND l.event_product = 'NEW'
    
    -- Nos aseguramos de tener una sola fila por compañía priorizando el primer demo
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.id_company ORDER BY sub.datestart ASC) = 1
),
*/
SELECT
id_company
,COUNT(*)
FROM demo_plan
GROUP BY 1
HAVING COUNT(*) > 1
