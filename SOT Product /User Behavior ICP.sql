WITH base AS (
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
        AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-09-01' --AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') <= '2025-12-31'
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
            AND event_time >= '2025-08-30'
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
        ,company_employees_adj
        ,company_phone
        FROM (
            SELECT
                id AS id_company
                ,employeesnumber AS company_employees
                ,CASE 
                    WHEN employeesnumber IN ('1') THEN '1'
                    WHEN employeesnumber IN ('1-2', '2-6', '3-6') THEN '2-10'
                    WHEN employeesnumber IN ('7-15', '16-25', '16-30') THEN '11-30'
                    WHEN employeesnumber IN ('26-50', 'Más de 30') THEN '31-50'
                    WHEN employeesnumber = '50+' THEN '51+'
                    ELSE NULL
                    END AS company_employees_adj
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
            --AND id_company = 2182491
    )
    
    ,pqls AS (
        SELECT
        id_company
        ,event_time
        ,event_time::date AS event_date
        ,event_name
        ,id_product
        FROM db_amplitude_events.amplitude_ac_events
        WHERE event_time >= '2025-01-01 00:00:00' -- Filtro sargable (rápido)
        AND (
            (event_time < '2025-08-01' AND event_name = 'ac-invoice-created')
            OR 
            (event_time >= '2025-08-01' AND event_name = 'ac-invoice-submitted')
        )
    )
    
    ,pqls_demo AS (
        SELECT 
            a.id_company, 
            a.id_product, 
            MIN(a.event_time)::date AS first_pql_date
        FROM pqls a
    
        INNER JOIN demo_plan AS b
            ON a.id_company = b.id_company
            AND a.id_product = b.id_product
        -- Traemos logos para saber si pagó antes de los 15 días
        LEFT JOIN logos AS c
            ON a.id_company = c.id_company
            AND a.id_product = c.id_product
    
        WHERE a.event_time >= b.demo_plan_start_date
          AND a.event_time <= CASE WHEN (c.logo_conversion_date IS NOT NULL AND c.logo_conversion_date < b.demo_plan_end_date) THEN c.logo_conversion_date ELSE b.demo_plan_end_date END
        GROUP BY 1, 2
    )
    --SQL: ´bi_sales.sql´
    
    
    /*
    SELECT
    id_company
    ,COUNT(*)
    FROM(*/
    SELECT *
    FROM (
        SELECT
        a.sign_up_type
        ,a.sign_up_date
        ,a.id_company
        ,a.country
        ,a.product_name
        ,a.id_product AS sign_up_id_product
        ,b.acquisition_channel_name AS sign_up_id_product_acquisition_channel_name
        ,CASE 
            WHEN b.acquisition_channel_name IN ('Paid Media (SEM + Social)', 'Influencers') 
                THEN 'Paid'
            WHEN b.acquisition_channel_name IN ('SEO / Organic Traffic', 'Direct Traffic', 'Referrals', 'Social Media (Organico)') 
                THEN 'Orgánico'
            WHEN b.acquisition_channel_name IN ('Lead Nurturing', 'Product Marketing', 'Others', 'Eventos', 'Alianzas') 
                THEN 'Baja Escala'
            ELSE 'Otros'
        END AS sign_up_id_product_acquisition_channel_category
        ,ba.sign_up_device_category
        ,c.event_date_onb_started
        ,c.event_date_role_selected
        ,d.company_profile
        ,c.event_date_user_info_filled
        ,e.user_company_position
        ,c.event_date_account_info_filled
        ,f.company_sector
        ,g.company_employees_adj
        ,g.company_phone
        ,CASE 
            WHEN a.country = 'colombia' THEN 
                CASE 
                    WHEN h.company_onb_revenue_tiers = 'Tier 1 Revenue' THEN '0-15M COP'
                    WHEN h.company_onb_revenue_tiers = 'Tier 2 Revenue' THEN '15M-50M COP'
                    WHEN h.company_onb_revenue_tiers = 'Tier 3 Revenue' THEN '50M+ COP'
                    ELSE h.company_onb_revenue_tiers
                END
            ELSE h.company_onb_revenue_tiers -- Lo que pasa si el país NO es Colombia
        END AS company_onb_revenue_tiers_adj
        ,c.event_date_onb_finished AS id_product_onb_finished_date
        ,ha.segment_type_onb
        ,i.id_plan
        ,i.plan_name
        ,i.internal_name
        ,i.demo_plan_start_date
        ,i.demo_plan_end_date AS id_product_demo_plan_end_date
        ,CASE WHEN j.logo_conversion_date < i.demo_plan_end_date THEN j.logo_conversion_date ElSE i.demo_plan_end_date END AS demo_plan_end_date_adj
        ,DATEDIFF('day',i.demo_plan_start_date,demo_plan_end_date_adj) AS id_product_num_demo_days
        ,j.logo_conversion_date AS id_product_purchase_conversion_date
        ,j.event_logo
        ,j.event_product
        ,k.fecha
        ,k.gestion
        ,k.detalle_gestion
        ,k.contactable AS id_product_sales_contact_type
        ,COALESCE(k.contactabilidad_adj,'Incontactable') AS contactabilidad_adj
        ,ROW_NUMBER() OVER (PARTITION BY a.id_company, a.id_product ORDER BY fecha DESC) AS rn
        ,l.segment_type_sales
        ,COALESCE(l.segment_type_sales, ha.segment_type_onb) AS segment_type_first_12_months
        ,m.first_pql_date AS id_product_first_pql_date
        ,CASE 
            -- Casos Orgánicos
            WHEN b.acquisition_channel_name IN ('SEO / Organic Traffic', 'Direct Traffic', 'Referrals', 'Social Media (Organico)') THEN
                CASE 
                    WHEN ba.sign_up_device_category = 'pc' THEN 'Organico + PC'
                    WHEN ba.sign_up_device_category = 'mobile' THEN 'Organico + Mobile'
                    ELSE 'Organico + Other Device' -- Para capturar 'other' si llega a pasar
                END
                
            -- Casos Paid
            WHEN b.acquisition_channel_name IN ('Paid Media (SEM + Social)', 'Influencers') THEN
                CASE 
                    WHEN ba.sign_up_device_category = 'pc' THEN 'Paid + PC'
                    WHEN ba.sign_up_device_category = 'mobile' THEN 'Paid + Mobile'
                    ELSE 'Paid + Other Device'
                END
                
            ELSE 'Non-ICP Profile' -- Baja escala u otros canales
        END AS icp_profile
        FROM fact_sign_ups AS a
    
        LEFT JOIN fact_acquisition_channel AS b
            ON a.id_company = b.id_company
            AND a.id_product = b.id_product
    
        LEFT JOIN register_device_category AS ba
            ON a.id_company = ba.id_company
    
        LEFT JOIN pivot_amplitude_onboarding AS c
            ON a.id_company = c.id_company
            AND a.id_product = c.id_product
    
        LEFT JOIN dim_subscribers AS d
            ON a.id_company = d.id_company
            AND c.event_date_role_selected IS NOT NULL
    
        LEFT JOIN alegra_users AS e
            ON a.id_company = e.id_company
            --AND c.event_date_user_info_filled IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN dim_subscribers AS f
            ON a.id_company = f.id_company
            --AND c.event_date_account_info_filled IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN alegra_companies AS g
            ON a.id_company = g.id_company
            --AND c.event_date_account_info_filled IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN revenue_tiers AS h
            ON a.id_company = h.id_company
            --AND c.event_date_account_info_filled IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN fact_sign_ups AS ha
            ON a.id_company = ha.id_company
            AND a.id_product = ha.id_product
            --AND c.event_date_onb_finished IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN demo_plan AS i
            ON a.id_company = i.id_company
            AND a.id_product = i.id_product
            --AND c.event_date_onb_finished IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN logos AS j
            ON a.id_company = j.id_company
            AND a.id_product = j.id_product
            --AND c.event_date_onb_finished IS NOT NULL
            AND d.company_profile = 'entrepreneur'
    
        LEFT JOIN sales_actions AS k
            ON a.id_company = k.id_company
            AND k.fecha >= i.demo_plan_start_date
            AND k.fecha <= CASE WHEN j.logo_conversion_date < i.demo_plan_end_date THEN j.logo_conversion_date ElSE i.demo_plan_end_date END
        
        LEFT JOIN dim_subscribers AS l
            ON a.id_company = l.id_company
            AND k.contactabilidad_adj = 'Contactable'
    
        LEFT JOIN pqls_demo AS m 
            ON a.id_company = m.id_company 
            AND a.id_product = m.id_product
    
        --WHERE a.id_company IN (2174798,2174815,2174836,2180833,2182491)
    )
    WHERE rn = 1
        AND id_product_onb_finished_date IS NOT NULL
        AND company_profile = 'entrepreneur'
        AND icp_profile IN ('Organico + PC','Organico + Mobile','Paid + PC','Paid + Mobile')
)

,demo_features AS (
    SELECT
    id_company
    ,event_name
    ,event_time
    ,event_time::date AS event_date
    FROM db_amplitude_events.amplitude_ac_events
    WHERE event_time >= '2025-01-01 00:00:00' -- Filtro global rápido
    AND (
        -- Rama A: Lógica condicional para facturas (Invoice)
        (event_time < '2025-08-01' AND event_name = 'ac-invoice-created')
        OR (event_time >= '2025-08-01' AND event_name = 'ac-invoice-submitted')
        
        -- Rama B: El resto de eventos que no cambian de nombre
        OR event_name IN (
            'ac-transaction-in-created',
            'ac-bill-created',
            'ac-report-visited',
            'ac-report-generated',
            'ac-item-created',
            'ac-first-step-managed',
            'ac-transaction-out-created',
            'ac-bank-created',
            'eco-wizard-finished',
            'ac-report-shared'
        )
    )
)

SELECT
b.sign_up_date
,b.id_company
,b.country
,b.product_name
,b.id_product
,b.acquisition_channel_name
,b.sign_up_device_category
,b.company_profile
,b.company_sector
,b.company_employees
,b.company_onb_revenue_tiers
,b.event_date_onb_finished
,b.demo_plan_end_date_adj
,b.demo_days
,b.logo_conversion_date
,b.contactabilidad_adj
,b.segment_type_first_12_months
,b.first_pql_date
,d.event_name
,d.event_date
,d.event_time
FROM base AS b

LEFT JOIN demo_features AS d
    ON b.id_company = d.id_company
    AND d.event_date >= b.event_date_onb_finished
    AND d.event_date <= b.demo_plan_end_date_adj

WHERE b.event_date_onb_finished IS NOT NULL
    AND b.id_company = 2119794

ORDER BY d.event_time
