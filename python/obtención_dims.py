# Obtención DIMs
df_b_dims = df_b.copy()
df_b_dims = df_b_dims[['Nombre País', 'País','ID Company','ID Company Perfil','ID Company Segmento','Mes de Registro','Fecha de Registro','Fecha fin del Demo','Fecha Último Login'
,'ID Company Device Registro','Fecha de conversión','ID Company pagó','ID Company con descuento en el pago']].drop_duplicates()

df_b_dims
