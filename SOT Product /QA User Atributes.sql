--- QAs SOT Producto ---
-- Canales --
SELECT
acquisition_channel_name
,COUNT(DISTINCT a.id_company) AS companies
FROM (
    SELECT
    a.country
    ,a.id_product
    ,a.id_company
    ,a.sign_up_date
    ,b.initial_utm_channel AS acquisition_channel_name
    FROM (
        SELECT
        id_company
        ,id_product
        ,TO_DATE(signup_date_key::text, 'YYYYMMDD') AS sign_up_date
        ,app_version AS country
        FROM bi_growth.bi_funnel_master_table_by_product
        WHERE id_product = 1
            AND TO_DATE(signup_date_key::text, 'YYYYMMDD') >= '2025-12-01'
            AND TO_DATE(signup_date_key::text, 'YYYYMMDD') <= '2025-12-31'
            AND app_version = 'colombia'
        GROUP BY 1,2,3,4
    ) AS a
    LEFT JOIN (
        SELECT
        id_company
        ,initial_utm_channel
        FROM data_table_bi.bi_growth.fact_attribution
        WHERE id_product = 1
            AND app_version = 'colombia'
        GROUP BY 1,2
    ) AS b
    ON a.id_company = b.id_company
) AS a
GROUP BY 1
