SELECT DISTINCT
a.country
,a.id_company AS id_company_contador
,a.sign_up_date
,a.event_type
,a.id_product
,a.id_company_type
,a.id_company_exp_group
,b.id_company_profile
,c.id_company AS id_company_pyme_asociada
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
/*
LEFT JOIN (
    SELECT DISTINCT
    id_ente_alegra
    ,id_company
    ,tipo_relacion -- SIN COMPANIES, ENTE PROPIO Y COMPANY ASOCIADA
    FROM bi_accountant.fact_accounting_sales_v1
) AS c
ON a.id_company = c.id_ente_alegra
*/
LEFT JOIN (
    -- A. PYMES VINCULADAS (CARTERA DE CLIENTES)
    SELECT DISTINCT 
        ae.hs_accounting_entity_id AS id_ente_hubspot, 
        ae.alegra_company_id AS id_ente_alegra,
        CAST(cr.hubspot_company_id AS BIGINT) AS id_company_hubspot, 
        CAST(cr.company_id AS BIGINT) AS id_company, 
        'COMPANY_ASOCIADA' AS tipo_relacion
    FROM db_hubspot.accounting_entities ae
    JOIN db_hubspot.associations_accounting_entities_to_companies aec 
        ON ae.hs_accounting_entity_id = aec.hs_accounting_entity_id
    JOIN db_hubspot.companies_relation_ids cr 
        ON aec.hs_company_id = cr.hubspot_company_id
    WHERE cr.company_id IS NOT NULL 
      -- 🛡️ FILTRO CRÍTICO ANTI-BUCLE (Validación 3):
      -- Excluye registros donde el ID del Hijo sea igual al ID del Padre.
      -- Si es NULL, pasa (es un lead puro). Si tiene dato, valida.
      AND (ae.alegra_company_id IS NULL OR CAST(cr.company_id AS BIGINT) != CAST(ae.alegra_company_id AS BIGINT))
    
    UNION
    
    -- B. ENTE PROPIO (EL CONTADOR COMO CLIENTE)
    SELECT DISTINCT 
        hs_accounting_entity_id AS id_ente_hubspot, 
        alegra_company_id AS id_ente_alegra,
        CAST(NULL AS BIGINT) AS id_company_hubspot, 
        alegra_company_id AS id_company, 
        'ENTE_PROPIO' AS tipo_relacion
    FROM db_hubspot.accounting_entities 
    WHERE alegra_company_id IS NOT NULL
    
    UNION
    
    -- C. ENTE SIN NADA (PROSPECTOS VACÍOS)
    -- Se incluyen para tener visibilidad del Pipeline aunque no facturen.
    SELECT DISTINCT 
        ae.hs_accounting_entity_id AS id_ente_hubspot, 
        ae.alegra_company_id AS id_ente_alegra,
        CAST(NULL AS BIGINT) AS id_company_hubspot, 
        CAST(NULL AS BIGINT) AS id_company, 
        'SIN_COMPANIES' AS tipo_relacion
    FROM db_hubspot.accounting_entities ae
    WHERE ae.alegra_company_id IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM db_hubspot.associations_accounting_entities_to_companies aec2
          JOIN db_hubspot.companies_relation_ids cr2 ON aec2.hs_company_id = cr2.hubspot_company_id
          WHERE aec2.hs_accounting_entity_id = ae.hs_accounting_entity_id AND cr2.company_id IS NOT NULL
      )
) AS c
ON a.id_company = c.id_ente_alegra
AND a.id_company <> c.id_company

LEFT JOIN (
    SELECT DISTINCT
    id_company
    ,date AS logo_date
    ,is_paying_logo
    FROM dwh_facts.fact_customers_mrr
    WHERE date >= '2025-07-21'
        AND app_version = 'colombia'
) AS d
ON c.id_company = d.id_company

--WHERE CASE WHEN c.id_ente_alegra IS NOT NULL THEN 'Si' ELSE 'No' END = 'No'

ORDER BY a.id_company, c.id_company
