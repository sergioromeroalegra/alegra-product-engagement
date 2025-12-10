SELECT *
FROM (
    SELECT
    a.country
    ,a.id_company
    ,a.sign_up_date
    ,a.id_product
    ,a.id_product_name
    ,a.demo_start_at
    ,a.demo_end_at
    ,a.id_plan
    ,a.id_company_plan_name
    ,a.prev_demo_start
    ,a.diff_days_demo
    ,MAX(a.id_with_1plus_demos) OVER (PARTITION BY a.id_company) AS id_with_1plus_demos
    FROM (
        SELECT
        a.country
        ,a.id_company
        ,a.sign_up_date
        ,a.id_product
        ,aa.id_product_name
        ,b.demo_start_at
        ,b.demo_end_at
        ,b.id_plan
        ,b.id_company_plan_name
        ,LAG(b.demo_start_at) OVER (PARTITION BY a.id_company, a.id_product ORDER BY b.demo_start_at, b.id_plan ASC) AS prev_demo_start
        ,DATEDIFF(day, LAG(b.demo_start_at) OVER (PARTITION BY a.id_company, a.id_product ORDER BY b.demo_start_at, b.id_plan ASC), b.demo_start_at) AS diff_days_demo
        ,CASE WHEN DATEDIFF(day, LAG(b.demo_start_at) OVER (PARTITION BY a.id_company, a.id_product ORDER BY b.demo_start_at, b.id_plan ASC), b.demo_start_at) > 0 THEN 'Si' ELSE 'No' END AS id_with_1plus_demos
        FROM (
            SELECT
            app_version AS country
            ,id_company
            --,CAST(DATE_TRUNC('year', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_year
            --,CAST(DATE_TRUNC('month', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_month
            ,TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD') AS sign_up_date
            ,segment_type_def AS id_company_segment --Segmento Core / Lite
            ,id_product
            FROM dwh_facts.fact_sign_ups 
            WHERE app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
                --AND event_type = 'LOGO' --Primer Sign Up
                --AND id_product = 1 --Alegra Contabilidad--
                AND id_date_registration_alegra >= 20250601
                --AND id_company = 1777410
        ) AS a

        JOIN (
            SELECT 1 AS id_product, 'Alegra Contabilidad' AS id_product_name
            UNION ALL SELECT 2, 'Alegra Tienda'
            UNION ALL SELECT 3, 'Alegra Nómina'
            UNION ALL SELECT 4, 'Alegra POS'
            UNION ALL SELECT 5, 'Tu Nómina Electrónica'
            UNION ALL SELECT 6, 'Alegra Facturación'
            UNION ALL SELECT 7, 'HolaBill'
            UNION ALL SELECT 8, 'E-Providers API'
            UNION ALL SELECT 9, 'Alegra Calcula'
            UNION ALL SELECT 11, 'Alegra Contador'
        ) AS aa
        ON a.id_product = aa.id_product

        JOIN (
            SELECT
            a.id_company
            ,a.demo_start_at
            ,a.demo_end_at
            ,a.id_plan
            ,b.id_company_plan_name
            FROM (
                SELECT
                idcompany AS id_company
                ,TRUNC(datestart) AS demo_start_at                    -- Inicio del período demo
                ,COALESCE(TRUNC(dateend), '9999-12-31') AS demo_end_at -- Fin del demo (o infinito)
                ,idplan AS id_plan
                FROM data_table_bi.db_membership.subscriptions
                --WHERE idcompany = '1777410'
            ) AS a

            JOIN (
                SELECT 
                id_plan
                ,plan_name AS id_company_plan_name
                FROM dwh_dimensions.dim_plan
                WHERE plan_name ILIKE '%demo%'
            ) AS b
            ON a.id_plan = b.id_plan

        ) AS b
        ON a.id_company = b.id_company
    ) AS a

) AS a

WHERE id_with_1plus_demos = 'Si'

ORDER BY a.sign_up_date,a.id_company,a.id_product, a.demo_start_at, a.id_plan
