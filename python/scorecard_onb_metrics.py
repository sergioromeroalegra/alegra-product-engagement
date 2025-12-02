# Tabla metrics de ONB
# 1. Filtrar solo eventos de ONB
df_b_metrics_onb = df_b[df_b['Categoría del Evento'] == 'Onboarding'].copy()
# 2. Nombre de eventos
# a. Diccionario nombres eventos
ajuste_nombres_eventos = {
    'ac-onboarding-started': 'Inicio Onboarding'
    ,'ac-role-selected': 'Rol Seleccionado'
    ,'ac-onb-user-information-filled': 'Info Usuario Completada'
    ,'ac-account-information-filled': 'Info Cuenta Completada'
    ,'ac-onboarding-finished': 'Fin Onboarding'
    ,'ac-first-step-managed': 'Primeros pasos Completado'
}
# b. aplicar diccionario
df_b_metrics_onb['Nombre del Evento'] = df_b_metrics_onb['Nombre del Evento'].map(ajuste_nombres_eventos)
# 3. Pivot Table
df_b_metrics_onb = df_b_metrics_onb.pivot_table(
    index='ID Company',
    columns='Nombre del Evento',
    values='Fecha del Evento',
    aggfunc='min'
)
# 4. Ajusta de columnas
# a. Orden
orden_logico_eventos = [
    'Inicio Onboarding',
    'Rol Seleccionado',
    'Info Usuario Completada',
    'Info Cuenta Completada',
    'Fin Onboarding',
    'Primeros pasos Completado'
]
# b. Ordenar
df_b_metrics_onb = df_b_metrics_onb.reindex(columns=orden_logico_eventos)
# c. Prefijo
df_b_metrics_onb.columns = ['Fecha ' + col for col in df_b_metrics_onb.columns]
# 5. Reset index
df_b_metrics_onb = df_b_metrics_onb.reset_index()

df_b_metrics_onb
