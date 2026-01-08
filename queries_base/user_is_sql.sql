SELECT DISTINCT
id_company
,fecha_sql AS sql_date
FROM bi_sales.sql
WHERE fecha_sql >= '2024-10-01 00:00:00' AND fecha_sql <= '2025-12-31 23:59:59'
