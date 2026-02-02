    SELECT
    a.id_company
    ,a.country
    ,a.product_name
    ,a.id_product
    ,a.sign_up_type
    ,a.sign_up_date
    ,a.segment_type_onb
    ,a.segment_type_def
    ,CASE WHEN b.acquisition_channel_name IS NULL THEN 'calculando...' ELSE b.acquisition_channel_name END AS acquisition_channel_name
    ,c.company_profile
    ,c.company_phone
    ,c.company_employees
    ,d.user_company_position
    ,e.company_onb_revenue_tiers
    FROM (
        SELECT
        a.country
        ,a.product_name
        ,a.id_product
        ,a.sign_up_type
        ,a.sign_up_date
        ,a.segment_type_onb
        ,a.segment_type_def
        ,a.id_company
        ,b.id_company_invited
        FROM (
            SELECT
            country
            ,product_name
            ,id_product
            ,sign_up_type
            ,sign_up_date
            ,id_company
            ,segment_type_onb
            ,segment_type_def
            FROM (
                SELECT
                app_version AS country
                ,product_name
                ,id_product
                ,CASE WHEN event_type = 'LOGO' THEN 'New User Alegra' ELSE 'New Subscriber Product' END AS sign_up_type -- LOGO, PRODUCT
                ,TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') AS sign_up_date
                ,id_company
                ,segment_type_onb
                ,segment_type_def
                ,ROW_NUMBER() OVER (PARTITION BY id_company, id_product ORDER BY CASE WHEN event_type = 'LOGO' THEN 1 ELSE 2 END ASC, id_date_registration_alegra ASC) AS rank_sign_up
                FROM dwh_facts.fact_sign_ups
                WHERE TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-12-01' AND TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') <= '2025-12-31'
                AND app_version IN ('colombia')
                AND id_product = 1
            ) AS a
            WHERE rank_sign_up = 1
            --AND id_company = 1967994
            --GROUP BY 1, 2, 3, 4, 5, 6
        ) AS a
        -- Eliminar registros de PyMes invitadas por contadores
        LEFT JOIN (
            SELECT DISTINCT
            a.id_contador
            --,b.id_company_invited
            ,b.email_company_invited
            ,b.invitation_status
            --,c.id_company AS id_company_invited_one
            ,CASE WHEN c.id_company IS NOT NULL AND b.id_company_invited <> c.id_company THEN c.id_company ELSE b.id_company_invited END AS id_company_invited
            FROM (
                SELECT DISTINCT
                ente_hubspot_id
                ,ente_alegra_id AS id_contador
                FROM bi_accountant.sales_actions_accountants
            ) AS a 

            JOIN (
                SELECT DISTINCT
                company_id AS id_contador
                ,company_id_invited AS id_company_invited
                ,sent_to_email AS email_company_invited
                ,status AS invitation_status
                FROM db_accountant.app_invitations_accountant
            ) AS b
            ON a.id_contador = b.id_contador

            LEFT JOIN (
                SELECT DISTINCT
                idcompany AS id_company
                ,email AS email_company
                FROM alegra.users
            ) AS c
            ON b.email_company_invited = c.email_company
        ) AS b
        ON a.id_company = b.id_company_invited

        WHERE b.id_company_invited IS NULL

    ) AS a
    -- Canal de llegada del usuario
    LEFT JOIN (
        SELECT
        id_company
        ,id_product
        ,initial_utm_channel AS acquisition_channel_name
        FROM bi_growth.fact_attribution
        WHERE app_version = 'colombia'
        GROUP BY 1,2,3
    ) AS b
    ON a.id_company = b.id_company
    AND a.id_product = b.id_product

    LEFT JOIN (
        SELECT
        id AS id_company
        ,employeesnumber AS company_employees
        ,phone AS company_phone
        ,sector AS company_sector
        ,profile AS company_profile
        FROM alegra.companies
        --GROUP BY 1, 2, 3
    ) AS c
    ON a.id_company = c.id_company
    -- Cargo del usuario dentro de la compañía
    LEFT JOIN (
        SELECT 
        idcompany AS id_company,
        dateregistry,
        JSON_EXTRACT_PATH_TEXT(metadata, 'position') AS user_company_position
        FROM alegra.users
        WHERE idlocal = 1
    ) AS d
    ON a.id_company = d.id_company

    LEFT JOIN (
        SELECT
        company_id AS id_company
        ,CASE WHEN mql_tier = 'Lite' THEN 'Tier 1 Revenue'
            WHEN mql_tier = 'Tier 2 Core' THEN 'Tier 2 Revenue'
            WHEN mql_tier = 'Tier 3 Core' THEN 'Tier 3 Revenue'
            ELSE 'Sin clasificacion'
            END AS company_onb_revenue_tiers
        FROM db_hubspot.mql_the_blip
    ) AS e
    ON a.id_company = e.id_company
