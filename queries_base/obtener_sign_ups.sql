SELECT
app_version AS country
,id_company
,TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD') AS sign_up_date
,segment_type_def AS id_company_segment --Segmento Core / Lite
FROM dwh_facts.fact_sign_ups 
WHERE app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
    AND event_type = 'LOGO' --Primer Sign Up
    AND id_product = 1 --Alegra Contabilidad--
    AND id_date_registration_alegra BETWEEN {fecha_inicio} AND {fecha_fin}
