SELECT
idcompany AS id_company
,device_type AS id_sign_up_device
FROM db_monolitico.mv_company_register_agents
WHERE datetime >= '{fecha_inicio} 00:00:00'
    AND datetime < '{fecha_fin} 00:00:00'
