# Definir ruta de archivos a trabajar
archivo = '/content/drive/MyDrive/Proyectos/Funnel Product Analysis/Sign Up a Fin Demo   Conversión/Reducción del Demo/Product Engagement Experimentos/Pronto Pago en Demo/comportamiento_id_companies.csv'
ruta_de_exportacion = '/content/drive/MyDrive/Proyectos/Funnel Product Analysis/Sign Up a Fin Demo   Conversión/Reducción del Demo/Product Engagement Experimentos/Pronto Pago en Demo/'

df_a = pd.read_csv(
    archivo
    # Convierte a Date las columnas
    ,parse_dates=['event_date','sign_up_date','demo_end_date']
    # Fuerza la carga a determinado data type
    ,dtype={
        'id_company' : str
        }
)

df_a
