# Cálculo de días activos
# 1. Crear copia
df_b_metrics_beh_dias = df_b_metrics_beh.copy()
# 2. Día del Demo en que sucedió el evento
df_b_metrics_beh_dias['Día del Demo en que usó el evento'] = (df_b_metrics_beh_dias['Fecha del Evento'] - df_b_metrics_beh_dias['Fecha de Registro']).dt.days +1
# 3. Subtabla para conteo
df_b_metrics_beh_dias = df_b_metrics_beh_dias[['ID Company','Día del Demo en que usó el evento']].drop_duplicates()
# 4. Filtrar solo eventos del D1 al D14
df_b_metrics_beh_dias = df_b_metrics_beh_dias[(df_b_metrics_beh_dias['Día del Demo en que usó el evento'] > 0) & (df_b_metrics_beh_dias['Día del Demo en que usó el evento'] < 15)]
# 5. Conteo de días activos
df_b_metrics_beh_dias = df_b_metrics_beh_dias.groupby('ID Company').agg(
    dias_activos_en_demo = ('Día del Demo en que usó el evento', 'nunique')
).reset_index()
# 6. Renombrar columna
df_b_metrics_beh_dias = df_b_metrics_beh_dias.rename(columns={'dias_activos_en_demo' : 'Días Activos en Demo'})

df_b_metrics_beh_dias

# Cálculo de Rachas (3 días o más consecutivos)
# 1. Crear copia
df_b_metrics_beh_racha = df_b_metrics_beh.copy()
# 2. Día del Demo en que sucedió el evento
df_b_metrics_beh_racha['Día del Demo en que usó el evento'] = (df_b_metrics_beh_racha['Fecha del Evento'] - df_b_metrics_beh_racha['Fecha de Registro']).dt.days +1
# 3. Subtabla para conteo
df_b_metrics_beh_racha = df_b_metrics_beh_racha[['ID Company','Día del Demo en que usó el evento']].drop_duplicates()
# 4. Filtrar solo eventos del D1 al D14
df_b_metrics_beh_racha = df_b_metrics_beh_racha[(df_b_metrics_beh_racha['Día del Demo en que usó el evento'] > 0) & (df_b_metrics_beh_racha['Día del Demo en que usó el evento'] < 15)]
# 5. Ordenar
df_b_metrics_beh_racha = df_b_metrics_beh_racha.sort_values(by=['ID Company', 'Día del Demo en que usó el evento'])
# 6. Encontrar las rachas
# a. Calculamos la diferencia de días con la fila anterior (para el mismo ID)
df_b_metrics_beh_racha['day_diff'] = df_b_metrics_beh_racha.groupby('ID Company')['Día del Demo en que usó el evento'].diff()
# b. Creamos un "ID de racha"
# i. Una nueva racha empieza CADA VEZ que la diferencia no es '1'
df_b_metrics_beh_racha['streak_id'] = (df_b_metrics_beh_racha['day_diff'] != 1).cumsum()
# 7. Agrupar por Streak_ID
df_b_metrics_beh_racha = df_b_metrics_beh_racha.groupby(['ID Company','streak_id']).agg({
    'Día del Demo en que usó el evento': 'count'
}).reset_index()
# 8. Crear condición
df_b_metrics_beh_racha['Estuvo 3 días o más consecutivos'] = np.where(df_b_metrics_beh_racha['Día del Demo en que usó el evento'] > 2, 'Yes','No')
# 6. Agrupar
df_b_metrics_beh_racha = df_b_metrics_beh_racha.groupby(['ID Company']).agg({
    'Estuvo 3 días o más consecutivos': 'max'
}).reset_index()

df_b_metrics_beh_racha

# Cálculo días que estuvo activo en Demo
# 1. Crear copia
df_b_metrics_beh_per = df_b_metrics_beh.copy()
# 2. Día del Demo en que sucedió el evento
df_b_metrics_beh_per['Día del Demo en que usó el evento'] = (df_b_metrics_beh_per['Fecha del Evento'] - df_b_metrics_beh_per['Fecha de Registro']).dt.days +1
# 3. Subtabla para conteo
df_b_metrics_beh_per = df_b_metrics_beh_per[['ID Company','Día del Demo en que usó el evento']].drop_duplicates()
# 4. Filtrar solo eventos del D1 al D14
df_b_metrics_beh_per = df_b_metrics_beh_per[(df_b_metrics_beh_per['Día del Demo en que usó el evento'] > 0) & (df_b_metrics_beh_per['Día del Demo en que usó el evento'] < 15)]
# 5. Ordenar
df_b_metrics_beh_per = df_b_metrics_beh_per.sort_values(by=['ID Company', 'Día del Demo en que usó el evento'])
# 6. Crear categorías
df_b_metrics_beh_per['Periodo días'] = np.where(df_b_metrics_beh_per['Día del Demo en que usó el evento'] < 3,'D1 a D2'
                                                 ,np.where(df_b_metrics_beh_per['Día del Demo en que usó el evento'] < 6,'D3 a D5'
                                                 ,np.where(df_b_metrics_beh_per['Día del Demo en que usó el evento'] < 11,'D6 a D10','D11 a D14')))
# 7. Pivotear
df_b_metrics_beh_per = df_b_metrics_beh_per.pivot_table(
    index = 'ID Company',
    columns ='Periodo días',
    values = 'Día del Demo en que usó el evento'
    ,aggfunc = 'nunique'
).fillna(0)
# 8. Resetear índice para que ID Company sea columna
df_b_metrics_beh_per.columns.name = None # Quita el nombre genérico del índice de columnas
df_b_metrics_beh_per = df_b_metrics_beh_per.reset_index()
# 9. Renombrar las columnas
df_b_metrics_beh_per = df_b_metrics_beh_per.rename(columns={
    'D1 a D2': 'Estuvo usando en periodo D1 a D2'
    ,'D3 a D5': 'Estuvo usando en periodo D3 a D5'
    ,'D6 a D10': 'Estuvo usando en periodo D6 a D10'
    ,'D11 a D14': 'Estuvo usando D11 a D14'
})
# 10. Convertir a "Yes/No" (Lógica final)
# Si el conteo es > 0, es 'Yes', si no, es 'No'.
cols_periodos = ['Estuvo usando en periodo D1 a D2','Estuvo usando en periodo D3 a D5','Estuvo usando en periodo D6 a D10','Estuvo usando D11 a D14']
for col in cols_periodos:
    # Verificamos si la columna existe (por si nadie usó el producto en algún periodo)
    if col in df_b_metrics_beh_per.columns:
        df_b_metrics_beh_per[col] = np.where(df_b_metrics_beh_per[col] > 0, 'Yes', 'No')
    else:
        # Si nadie usó en ese periodo, creamos la columna llena de 'No'
        df_b_metrics_beh_per[col] = 'No'
# 11. Ajustar orden de cols
column_order = ['ID Company','Estuvo usando en periodo D1 a D2','Estuvo usando en periodo D3 a D5','Estuvo usando en periodo D6 a D10','Estuvo usando D11 a D14']
df_b_metrics_beh_per = df_b_metrics_beh_per[column_order]

df_b_metrics_beh_per
