-- Query Cross Sell
SELECT
DATE_TRUNC('month', date) AS date_month
    ,COUNT(DISTINCT CASE WHEN event_logo = 'EXPANSION CROSS SELLING' THEN id_company END) AS usuarios_con_cross_sell
    ,COUNT(DISTINCT CASE WHEN is_paying_logo = 'yes' THEN id_company END) AS base_usuarios_activos
    ,COUNT(DISTINCT CASE WHEN event_logo = 'EXPANSION CROSS SELLING' THEN id_company END)::FLOAT / NULLIF(COUNT(DISTINCT CASE WHEN is_paying_logo = 'yes' THEN id_company END), 0) AS tasa_cross_sell
FROM dwh_facts.fact_customers_mrr
WHERE date >= '2025-09-01'
    AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
GROUP BY DATE_TRUNC('month', date);
-- Query Multi Producto
SELECT
    date_month
    ,COUNT(DISTINCT CASE WHEN num_productos_activos > 1 THEN id_company END) AS usuarios_multi_producto
    ,COUNT(DISTINCT CASE WHEN num_productos_activos >= 1 THEN id_company END) AS base_usuarios_activos
    ,COUNT(DISTINCT CASE WHEN num_productos_activos > 1 THEN id_company END)::FLOAT 
     / NULLIF(COUNT(DISTINCT CASE WHEN num_productos_activos >= 1 THEN id_company END), 0) AS porcentaje_multi_producto
FROM (
    SELECT
    DATE_TRUNC('month', date) AS date_month
    ,id_company
        ,COUNT(DISTINCT CASE WHEN is_paying_product = 'yes' THEN id_product END) AS num_productos_activos
    FROM dwh_facts.fact_customers_mrr
    WHERE date >= '2025-09-01'
        AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
    GROUP BY 1, 2
) AS a 
GROUP BY date_month
ORDER BY date_month DESC;

-- Query para Cross-Sell en Amplitude
SELECT 
CAST(a.id_company AS VARCHAR) AS user_id
,a.date_month 
,'manual_cross_sell_backend' AS event_type       -- La Llave Maestra
,a.active_products_count                       -- El Estado (LTV)
,COALESCE(a.product_mix, 'Ninguno') AS product_mix -- Contexto
,CASE WHEN b.multi_product_activation_date IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_cross_sell                -- La Tendencia
FROM (
-- SUBCONSULTA 1 (Externa): Agregamos (Count y Listagg) sobre datos ya únicos
    SELECT 
    id_company
    ,date_month
        ,COUNT(product_name) AS active_products_count -- Ya no necesitamos DISTINCT aquí
        ,LISTAGG(product_name, ' + ') WITHIN GROUP (ORDER BY product_name) AS product_mix
    FROM (
        -- SUBCONSULTA 1.1 (Interna): Limpiamos duplicados primero
        SELECT DISTINCT 
        id_company
        ,CAST(DATE_TRUNC('month',date) AS DATE) AS date_month
        ,product_name
        --SELECT *
        FROM dwh_facts.fact_customers_mrr
        WHERE date >= '2025-06-01' AND date <= '2025-07-31'
          AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
          AND id_company = '1001502'
          AND is_paying_product = 'yes' -- Filtramos solo los que pagan
    ) AS a
    GROUP BY id_company, date_month
) AS a

LEFT JOIN (
    SELECT
    id_company
    ,CAST(DATE_TRUNC('month',date) AS DATE) AS date_month
        ,MIN(date) AS multi_product_activation_date
    FROM dwh_facts.fact_customers_mrr
    WHERE event_logo = 'EXPANSION CROSS SELLING'
    GROUP BY id_company, CAST(DATE_TRUNC('month',date) AS DATE)
) AS b
ON a.id_company = b.id_company
AND a.date_month = b.date_month

WHERE a.active_products_count >= 1 --AND CAST(a.id_company AS VARCHAR) = '1001502'
ORDER BY CAST(a.id_company AS VARCHAR),a.date_month;
