from dagster import Definitions, define_asset_job, AssetSelection

from .assets import (
    leer_datos,
    datos_procesados,
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    reporte_excel_covid,  # Paso 6
    chequeos_entrada,
    validar_datos_procesados,
    validar_metrica_incidencia_7d,
    validar_metrica_factor_crec_7d,
)

# Job que incluye el Excel final y todos los upstream
pipeline_completo = define_asset_job(
    "pipeline_completo",
    selection=AssetSelection
        .keys("reporte_excel_covid")  # dispara todo lo necesario para llegar al Excel
        .upstream()
)

# Definición principal de Dagster
defs = Definitions(
    assets=[
        leer_datos,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid,  # Paso 6
    ],
    asset_checks=[
        chequeos_entrada,
        validar_datos_procesados,
        validar_metrica_incidencia_7d,
        validar_metrica_factor_crec_7d,
    ],
    jobs=[pipeline_completo],
)
