SELECT 
    b.event_name,
    a.total_registros_rs AS total_universo_periodo,
    COUNT(DISTINCT b.id_company) AS ids_encontrados_amplitude,
    ROUND(COUNT(DISTINCT b.id_company)::FLOAT / NULLIF(a.total_registros_rs * 0.96, 0) * 100, 2) AS pct_conversion_total
FROM (
    -- Subquery A: Una sola fila con el total de registros únicos en el periodo
    SELECT 
        COUNT(DISTINCT id_company) AS total_registros_rs
    FROM dwh_facts.fact_sign_ups
    WHERE id_date_registration_alegra BETWEEN 20250701 AND 20260131
      AND id_product = 1
      AND app_version IN ('colombia','costaRica','republicaDominicana','mexico')
) AS a
CROSS JOIN (
    -- Subquery B: IDs únicos que hicieron cada evento (sin importar el mes)
    -- Pero filtrados para que solo cuenten si existen en el universo de registros
    SELECT 
        ev.event_name,
        ev.id_company
    FROM (
        SELECT event_time, event_name, id_company FROM db_amplitude_events.amplitude_ac_events
        UNION ALL 
        SELECT event_time, event_name, id_company FROM db_amplitude_events.amplitude_attribution
    ) AS ev
    -- Este INNER JOIN asegura que solo contamos gente que se registró en el periodo de interés
    INNER JOIN dwh_facts.fact_sign_ups AS rs ON ev.id_company = rs.id_company
    WHERE rs.id_date_registration_alegra BETWEEN 20250701 AND 20260131
      AND rs.id_product = 1
      AND rs.app_version IN ('colombia','costaRica','republicaDominicana','mexico')
      AND ev.event_name IN ('ac-account-information-filled','ac-first-step-managed','ac-onb-user-information-filled','ac-onboarding-finished','ac-onboarding-started','ac-role-selected'
      ,'ac-item-created','eco-wizard-finished','ac-invoice-submitted','ac-bill-created','ac-report-generated','ac-transaction-in-created','ac-support-document-created','ac-report-visited')
) AS b
GROUP BY 1, 2
ORDER BY 3 DESC;
