SELECT DISTINCT
a.country
,a.id_company AS id_company_contador
,a.sign_up_date
,a.event_type
,a.id_product
,a.id_company_type
,a.id_company_exp_group
,b.id_company_profile
,c.id_company_invited
,c.invitation_status
,d.is_paying_logo AS id_company_is_paying
FROM (
    SELECT DISTINCT
    app_version AS country
    ,id_company
    ,CASE WHEN id_company ~ '[02468]$' THEN 'Par' ELSE 'Impar' END AS id_company_type
    ,CASE WHEN id_company ~ '[02468]$' THEN 'JTBD' ELSE 'EC/AC' END AS id_company_exp_group
    --,CAST(DATE_TRUNC('year', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_year
    --,CAST(DATE_TRUNC('month', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_month
    ,TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD') AS sign_up_date
    ,event_type -- PRODUCT, LOGO (Primer Registro)
    ,id_product
    --,segment_type_def AS id_company_segment --Segmento Core / Lite
    FROM dwh_facts.fact_sign_ups 
    WHERE app_version = 'colombia'
        AND event_type = 'LOGO' --Primer Sign Up
        AND id_product = 1 --Alegra Contabilidad--
        AND id_date_registration_alegra BETWEEN 20250721 AND 20251031
) AS a

JOIN (
    SELECT DISTINCT
    idcompany AS id_company
    ,profile AS id_company_profile --perfil entrepreneur, accountant, independent, student
    ,COALESCE(segment_type_sales,segment_type_onb) AS id_company_segment
    FROM dwh_dimensions.dim_subscribers
    WHERE appversion = 'colombia'
        AND registration_date >= '2025-07-21 00:00:00'
        AND registration_date < '2025-11-01 00:00:00'
        AND profile = 'accountant'
) AS b
ON a.id_company = b.id_company

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
) AS c
ON a.id_company = c.id_contador

LEFT JOIN (
    SELECT DISTINCT
    id_company
    ,date AS logo_date
    ,is_paying_logo
    FROM dwh_facts.fact_customers_mrr
    WHERE date >= '2025-07-21'
        AND app_version = 'colombia'
) AS d
ON c.id_company_invited = d.id_company

--WHERE c.id_company_invited <> c.id_company_invited_one

ORDER BY a.id_company, c.id_company_invited
