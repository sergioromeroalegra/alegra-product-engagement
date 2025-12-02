# Unión Dims con Metrics ONB
scorecard = pd.merge(df_b_dims, df_b_metrics_onb, on='ID Company', how='left')
# Unión con Beh Días
scorecard = pd.merge(scorecard, df_b_metrics_beh_dias, on='ID Company', how='left')
# Unión con Beh Racha
scorecard = pd.merge(scorecard, df_b_metrics_beh_racha, on='ID Company', how='left')
# Unión con Beh Periodos
scorecard = pd.merge(scorecard, df_b_metrics_beh_per, on='ID Company', how='left')
# Unión con Beh Total
scorecard = pd.merge(scorecard, df_b_metrics_beh_total, on='ID Company', how='left')
# Unión con Beh Categoría Fechas
scorecard = pd.merge(scorecard, df_b_metrics_beh_cat_fecha, on='ID Company', how='left')
# Unión con Beh Categoría Uso
scorecard = pd.merge(scorecard, df_b_metrics_beh_cat_uso, on='ID Company', how='left')

# Columnas
# 1. Ajustar orden de Columnas
scorecard_orden_columnas = [
    # Dims
    'Nombre País', 'País', 'ID Company', 'ID Company Perfil','ID Company Segmento', 'Mes de Registro', 'Fecha de Registro','Fecha fin del Demo','Fecha Último Login'
    ,'ID Company Device Registro','Fecha de conversión', 'ID Company pagó','ID Company con descuento en el pago'
    # Métricas ONB
    ,'Fecha Inicio Onboarding','Fecha Rol Seleccionado', 'Fecha Info Usuario Completada','Fecha Info Cuenta Completada'
    ,'Fecha Fin Onboarding','Fecha Primeros pasos Completado'
    # Métricas Días
    ,'Días Activos en Demo'
    # Métricas Racha
    ,'Estuvo 3 días o más consecutivos'
    # Métricas Periodos
    ,'Estuvo usando en periodo D1 a D2','Estuvo usando en periodo D3 a D5','Estuvo usando en periodo D6 a D10','Estuvo usando D11 a D14'
    # Métricas Total
    ,'Fecha de 1er uso de features','Número de veces uso de features', 'Fecha de 1er uso de features PQL','Número de veces uso de features PQL'
    # Métricas Fechas y Uso
    ,'Fecha 1ra vez feature de Bancos','No. de veces uso de feature de Bancos','Fecha 1ra vez feature de Contabilidad','No. de veces uso de feature de Contabilidad'
    ,'Fecha 1ra vez feature de Factura Compra','No. de veces uso de feature de Factura Compra','Fecha 1ra vez feature de Factura Venta','No. de veces uso de feature de Factura Venta'
    ,'Fecha 1ra vez feature de Reporte','No. de veces uso de feature de Reporte','Fecha 1ra vez feature de Transacción','No. de veces uso de feature de Transacción'
]
# 2. Aplicar Orden
scorecard = scorecard[scorecard_orden_columnas]

# Exportar a CSV
scorecard.to_csv(
    ruta_de_exportacion + 'scorecard_pronto_pago_en_demo.csv'
    ,index=False
)

scorecard

