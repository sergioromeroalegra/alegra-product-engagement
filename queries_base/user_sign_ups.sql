SELECT
app_version AS country
,id_company
--,CAST(DATE_TRUNC('year', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_year
--,CAST(DATE_TRUNC('month', TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD')) AS DATE) AS sign_up_month
,TO_DATE(CAST(id_date_registration_alegra AS VARCHAR), 'YYYYMMDD') AS sign_up_date
,segment_type_def AS id_company_segment_onb --Segmento Core / Lite
FROM dwh_facts.fact_sign_ups 
WHERE app_version IN ('colombia','costaRica','republicaDominicana','mexico','argentina','peru')
    AND event_type = 'LOGO' --Primer Sign Up
    AND id_product = 1 --Alegra Contabilidad--
    AND id_date_registration_alegra BETWEEN  20250701 AND 20251130 -- sin comillas en formato YYYYMMDD
