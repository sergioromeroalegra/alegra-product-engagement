# Tabla metrics de Comportamiento
# 1. Filtrar solo eventos de Comportamiento
df_b_metrics_beh = df_b[df_b['Categoría del Evento'] != 'Onboarding'].copy()
# 2. Agrupar por Company ID el primer evento Beh
df_b_metrics_beh_total = df_b_metrics_beh.assign(
    fecha_pql_temp = np.where(
        df_b_metrics_beh['Evento es PQL'] == 'Yes', 
        df_b_metrics_beh['Fecha del Evento'], 
        pd.NaT
    ),
    nombre_pql_temp = np.where(
        df_b_metrics_beh['Evento es PQL'] == 'Yes' 
        ,df_b_metrics_beh['Nombre del Evento'] 
        ,np.nan
    )
).groupby('ID Company').agg(
    primer_uso_de_features = ('Fecha del Evento', 'min')
    ,num_veces_uso_features = ('Nombre del Evento', 'count')
    ,primer_uso_de_features_pql = ('fecha_pql_temp', 'min')
    ,num_veces_uso_features_pql = ('nombre_pql_temp', 'count')
).reset_index()
# 3. Ajustar a Date
df_b_metrics_beh_total['primer_uso_de_features_pql'] = pd.to_datetime(
    df_b_metrics_beh_total['primer_uso_de_features_pql']
)
# 4. Ajustar nombre de columnas
df_b_metrics_beh_total = df_b_metrics_beh_total.rename(columns={
  'primer_uso_de_features': 'Fecha de 1er uso de features'
  ,'num_veces_uso_features': 'Número de veces uso de features'
  ,'primer_uso_de_features_pql': 'Fecha de 1er uso de features PQL'
  ,'num_veces_uso_features_pql': 'Número de veces uso de features PQL'
  })


df_b_metrics_beh_total
