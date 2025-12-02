# Listas
# 1. Lista de eventos AC PQL
pql_events_list = [
    'ac-invoice-submitted',
    'ac-invoice-created',
    'ac-bill-created',
    'ac-support-document-created',
    'ac-transaction-in-created',
    'ac-transaction-out-created',
    'ac-report-generated',
    'ac-report-visited'
]

# Diccionarios
# 1. Eventos y categoría pre-definida
eventos_categoría = {
'ac-account-information-filled' : 'Onboarding', 'ac-administrator-xml-imported-solicited' : 'Contabilidad', 'ac-bank-created' : 'Bancos', 'ac-bank-reconciliation-finished' : 'Bancos'
, 'ac-bill-created' : 'Factura Compra', 'ac-first-step-managed' : 'Onboarding', 'ac-invoice-created' : 'Factura Venta', 'ac-invoice-submitted' : 'Factura Venta'
, 'ac-item-created' : 'Factura Venta', 'ac-journal-created' : 'Contabilidad', 'ac-ledger-category-import-started' : 'Contabilidad', 'ac-onb-user-information-filled' : 'Onboarding'
, 'ac-onboarding-finished' : 'Onboarding', 'ac-onboarding-started' : 'Onboarding', 'ac-opening-balance-created' : 'Contabilidad', 'ac-opening-balance-import-finished' : 'Contabilidad'
, 'ac-report-generated' : 'Reporte', 'ac-report-shared' : 'Reporte', 'ac-report-visited' : 'Reporte', 'ac-role-selected' : 'Onboarding', 'ac-send-company-invitation' : 'Primeros Pasos'
, 'ac-support-document-created' : 'Contabilidad', 'ac-transaction-in-created' : 'Transacción', 'ac-transaction-out-created' : 'Transacción', 'eco-wizard-finished' : 'Factura Venta'
}

# 2. Nombres de los paises
abreviaciones_paises = {
    'colombia' : 'COL'
    ,'mexico' : 'MEX'
    ,'costaRica' :'CRI'
    ,'republicaDominicana' : 'DOM'
    ,'argentina' : 'ARG'
    ,'peru': 'PER'
    ,'panama' : 'PAN'
}

# Eventos en dataset
eventos = df_a[['event_name','event_type']].copy().drop_duplicates()
# Unir con categoría
eventos['Categoría del Evento'] = eventos['event_name'].map(eventos_categoría)
#Definir si son PQL
eventos['Evento es PQL'] = np.where(eventos['event_name'].isin(pql_events_list), 'Yes', 'No')
# Ordenar
eventos.sort_values(by=['event_type','event_name'])
