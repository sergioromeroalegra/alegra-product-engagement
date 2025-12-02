# Agregar abreviaciones de países
df_a['País'] = df_a['country'].map(abreviaciones_paises)

# Agregar categoría de eventos
df_b = pd.merge(df_a, eventos[['event_name','Categoría del Evento','Evento es PQL']], on='event_name', how='left')

#Eliminar columna
df_b = df_b.drop(columns=['event_type'])

# Ajustar nombres de Columnas
df_b.columns = [
    'Nombre País','ID Company','Mes de Registro','Fecha de Registro','Fecha fin del Demo','ID Company Perfil','ID Company Segmento','Fecha de conversión','ID Company pagó'
    ,'ID Company con descuento en el pago','ID Company Device Registro','Fecha Último Login'
    # Eventos
    ,'Nombre del Evento','Fecha del Evento','Timestamp del Evento'
    # acá las variables creadas en Python
    ,'País','Categoría del Evento','Evento es PQL'
]

# Ajustar Orden de columnas
orden_columnas = [
    'Nombre País', 'País','ID Company','ID Company Perfil','ID Company Segmento','Mes de Registro','Fecha de Registro','Fecha fin del Demo','Fecha Último Login','ID Company Device Registro'
    ,'Fecha de conversión','ID Company pagó','ID Company con descuento en el pago','Nombre del Evento','Fecha del Evento','Timestamp del Evento','Categoría del Evento','Evento es PQL'
]
df_b = df_b[orden_columnas]

df_b
