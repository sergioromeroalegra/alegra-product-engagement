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
