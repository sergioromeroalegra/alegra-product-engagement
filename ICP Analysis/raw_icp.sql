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
    FROM (
        SELECT 
            id_company, 
            initial_utm_channel AS id_product_acquisition_channel_name,
            ROW_NUMBER() OVER (PARTITION BY id_company ORDER BY initial_utm_channel DESC) as rn
        FROM bi_growth.fact_attribution
        WHERE 
            id_product = 1 
            AND app_version = 'colombia'
    ) WHERE rn = 1
)

,onboarding_atributes_1 AS (
    SELECT 
        id_company
        ,company_employees_adj
        ,company_phone
    FROM (
        SELECT 
            id AS id_company,
            phone AS company_phone,
            CASE 
                WHEN employeesnumber IN ('1') THEN '1'
                WHEN employeesnumber IN ('1-2', '2-6', '3-6') THEN '2-10'
                WHEN employeesnumber IN ('7-15', '16-25', '16-30') THEN '11-30'
                WHEN employeesnumber IN ('26-50', 'Más de 30') THEN '31-50'
                WHEN employeesnumber = '50+' THEN '51+'
                ELSE NULL 
            END AS company_employees_adj,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) as rn
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
            company_id AS id_company,
            CASE 
                WHEN mql_tier = 'Lite' THEN '0-15M COP'
                WHEN mql_tier = 'Tier 2 Core' THEN '15M-50M COP'
                WHEN mql_tier = 'Tier 3 Core' THEN '50M+ COP'
                ELSE 'Sin clasificacion'
            END AS company_onb_revenue_tiers,
            ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY last_modified DESC) AS rn
        FROM db_hubspot.mql_the_blip
    ) 
    WHERE rn = 1
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

,user_position AS (
    SELECT 
    id_company
    ,user_company_position
    FROM (
        SELECT 
            idcompany AS id_company,
            JSON_EXTRACT_PATH_TEXT(metadata, 'position') AS user_company_position,
            ROW_NUMBER() OVER (PARTITION BY idcompany ORDER BY id) as rn -- Safety net
        FROM alegra.users
        WHERE idlocal = 1
    ) WHERE rn = 1
)

,onb_finished AS (
    SELECT 
        id_company, 
        MIN(event_time)::date AS id_product_onb_finish_date
    FROM db_amplitude_events.amplitude_attribution
    WHERE event_name = 'ac-onboarding-finished'
      AND event_time >= '2025-09-01'
      AND id_product = 1
    GROUP BY 1
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
            WHEN detalle_gestion ~* 'cuelga|incorrecto|sin datos|errado|no.*comunicaci.n|incontactable'
                    THEN 'Incontactable'
            
            -- 2. NO PERFIL / JUNK
            WHEN detalle_gestion ~* 'duplicada|estudiante|competencia|lista negra|prueba' 
                    THEN 'No Perfil (Junk)'
            
            -- 3. CONTACTADO
            ELSE 'Contactable'
        END AS contactabilidad_adj
    FROM bi_sales.sales_actions
    WHERE gestion IN ('Onboarding terminado','Lead descalificado','Leads contactables')
)

,sales_actions_adj AS (
    SELECT *
    FROM (
        SELECT 
            a.id_company
            ,a.id_product_demo_start_date
            ,a.id_product_demo_end_date_adj
            ,b.fecha::date AS contact_date
            ,b.gestion
            ,b.detalle_gestion
            ,CASE 
                WHEN b.detalle_gestion ~* 'cuelga|incorrecto|sin datos|errado|no.*comunicaci.n|incontactable' THEN 'Incontactable'
                WHEN b.detalle_gestion ~* 'duplicada|estudiante|competencia|lista negra|prueba' THEN 'No Perfil (Junk)'
                ELSE 'Contactable'
            END AS contactabilidad_adj
            ,ROW_NUMBER() OVER (PARTITION BY a.id_company ORDER BY b.fecha DESC) AS rn
        FROM demo_period_adj AS a

        INNER JOIN sales_actions b 
            ON a.id_company = b.id_company
            AND b.fecha >= a.id_product_demo_start_date
            AND b.fecha <= a.id_product_demo_end_date_adj
    )
    -- IMPORTANTE: Aquí nos quedamos solo con la ÚLTIMA gestión dentro de la ventana
    WHERE rn = 1
)

SELECT
    a.country
    ,a.id_company
    ,a.id_product
    ,a.id_product_sign_up_date
    ,b.id_product_acquisition_channel_name
    ,e.sign_up_device_category
    ,c.company_employees_adj
    ,c.company_phone
    ,a.company_sector
    ,d.company_onb_revenue_tiers
    ,f.user_company_position
    ,a.segment_type_onb
    ,a.segment_type_sales
    ,COALESCE(
        g.id_product_onb_finish_date, -- Prioridad 1: Frontend
        h.id_product_first_pql_date,  -- Prioridad 2: PQL
        i.id_product_purchase_date    -- Prioridad 3: Compra
    ) AS id_product_onb_finish_date_adj
    ,h.id_product_first_pql_date
    ,i.id_product_purchase_date
    ,j.id_product_demo_start_date
    ,j.id_product_demo_end_date_adj
    ,k.contact_date
    ,k.contactabilidad_adj
FROM entrepreneurs AS a
LEFT JOIN acquisition_channel AS b ON a.id_company = b.id_company
LEFT JOIN onboarding_atributes_1 AS c ON a.id_company = c.id_company
LEFT JOIN revenue_tiers AS d ON a.id_company = d.id_company 
LEFT JOIN sign_up_device AS e ON a.id_company = e.id_company
LEFT JOIN user_position AS f ON a.id_company = f.id_company
LEFT JOIN onb_finished AS g ON a.id_company = g.id_company
LEFT JOIN pqls AS h ON a.id_company = h.id_company
LEFT JOIN logos AS i ON a.id_company = i.id_company
LEFT JOIN demo_period_adj AS j ON a.id_company = j.id_company
LEFT JOIN sales_actions_adj AS k ON a.id_company = k.id_company



