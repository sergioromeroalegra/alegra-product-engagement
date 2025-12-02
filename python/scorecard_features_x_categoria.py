# Tabla metrics de Comportamiento por Categoría Fechas
# 1. Pivot Table Fecha
df_b_metrics_beh_cat_fecha = df_b_metrics_beh.pivot_table(
    index='ID Company',
    columns='Categoría del Evento',
    values='Fecha del Evento',
    aggfunc='min'
)
# a. Prefijo
df_b_metrics_beh_cat_fecha.columns = ['Fecha 1ra vez feature de ' + col for col in df_b_metrics_beh_cat_fecha.columns]
# 2. Reset index
df_b_metrics_beh_cat_fecha = df_b_metrics_beh_cat_fecha.reset_index()

df_b_metrics_beh_cat_fecha.columns

# Tabla metrics de Comportamiento por Categoría Uso
# 1. Pivot Table Fecha
df_b_metrics_beh_cat_uso = df_b_metrics_beh.pivot_table(
    index='ID Company',
    columns='Categoría del Evento',
    values='Fecha del Evento',
    aggfunc='count'
)
# a. Prefijo
df_b_metrics_beh_cat_uso.columns = ['No. de veces uso de feature de ' + col for col in df_b_metrics_beh_cat_uso.columns]
# 2. Reset index
df_b_metrics_beh_cat_uso= df_b_metrics_beh_cat_uso.reset_index()

df_b_metrics_beh_cat_uso.columns
