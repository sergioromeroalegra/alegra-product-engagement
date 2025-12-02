SELECT DISTINCT
    id_company
    ,date_month
    ,event_logo
    ,date AS churn_date
FROM dwh_facts.fact_customers_mrr
WHERE date >='{fecha_inicio}'
    AND app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
    AND id_product = 1 
    AND event_logo  = 'CHURN'
