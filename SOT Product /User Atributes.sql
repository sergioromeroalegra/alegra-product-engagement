--- User Atributes ---
-- Trear Atributes al momento del sign up del producto --

-- Revisión de duplicados --
/*
SELECT
id_company
,COUNT(*)
FROM (
*/
    SELECT
    a.id_company
    ,a.country
    ,a.product_name
    ,a.id_product
    ,CASE WHEN b.acquisition_channel_name IS NULL THEN 'calculando...' ELSE b.acquisition_channel_name END AS acquisition_channel_name
    ,a.sign_up_type
    ,a.sign_up_date
    ,c.company_employees
    ,c.company_phone
    ,d.user_company_position
    FROM (
        SELECT
        app_version AS country
        ,product_name
        ,id_product
        ,CASE WHEN event_type = 'LOGO' THEN 'New User Alegra' ELSE 'New Subscriptor Product' END AS sign_up_type -- LOGO, PRODUCT
        ,TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') AS sign_up_date
        ,id_company
        FROM dwh_facts.fact_sign_ups
        WHERE TO_DATE(id_date_registration_alegra::text, 'YYYYMMDD') >= '2025-06-01'
        AND app_version IN ('colombia')
        AND id_product = 1
        AND id_company = 1967994
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id_company 
            ORDER BY 
                CASE WHEN event_type = 'LOGO' THEN 1 ELSE 2 END ASC, -- Prioridad 1: New User Alegra
                id_date_registration_alegra ASC                    -- Prioridad 2: La fecha más antigua (por si acaso)
        ) = 1

        --GROUP BY 1, 2, 3, 4, 5, 6
    ) AS a

    LEFT JOIN (
        SELECT
        id_product
        ,channel_name AS acquisition_channel_name
        ,TO_DATE(date_id::text, 'YYYYMMDD') AS acquisition_channel_date
        ,id_company
        FROM bi_growth.fact_acquisition_channels_companies
        --WHERE id_company = 2130266
        GROUP BY 1, 2, 3, 4
    ) AS b
    ON a.id_company = b.id_company
    AND a.id_product = b.id_product

    LEFT JOIN (
        SELECT
        id AS id_company
        ,employeesnumber AS company_employees
        ,phone AS company_phone
        FROM alegra.companies
        GROUP BY 1, 2, 3
    ) AS c
    ON a.id_company = c.id_company

    LEFT JOIN (
        SELECT
        idcompany AS id_company
        ,JSON_EXTRACT_PATH_TEXT(metadata, 'position') AS user_company_position
        FROM alegra.users
        GROUP BY 1, 2
    ) AS d
    ON a.id_company = d.id_company

    --WHERE a.id_company = 1967994

    --ORDER BY a.sign_up_date
/*
) AS a

GROUP BY 1

HAVING COUNT(*) > 1
*/
