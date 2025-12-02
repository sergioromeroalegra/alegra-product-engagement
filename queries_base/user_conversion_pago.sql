SELECT DISTINCT
id_company
,date AS logo_date
,CASE WHEN amount_discount IS NULL OR amount_discount = 0 THEN 'No' ELSE 'Yes' END AS id_company_used_discount
FROM dwh_facts.fact_customers_mrr
WHERE date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
    AND id_product = 1 
    AND event_logo = 'NEW' -- Primer Pago en Alegra
