from dagster import asset, asset_check, AssetCheckResult, MetadataValue, Output, TableSchema, TableColumn, TableRecord
import pandas as pd
import datetime as dt
import requests
from io import StringIO
import numpy as np

URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"

@asset
def leer_datos() -> pd.DataFrame:
    """Lee el CSV de OWID y devuelve un DataFrame."""
    resp = requests.get(URL)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    df = df.rename(columns={"country": "location"})
    return df

@asset_check(asset="leer_datos")
def chequeos_entrada(context, leer_datos: pd.DataFrame):
    df = leer_datos.copy()
    hoy = pd.Timestamp(dt.date.today())

    resumen = []

    # 1. No fechas futuras
    mask_futuras = pd.to_datetime(df['date']) > hoy
    resumen.append({
        "nombre_regla": "No fechas futuras",
        "estado": not mask_futuras.any(),
        "filas_afectadas": int(mask_futuras.sum()),
        "notas": "Fechas > hoy detectadas" if mask_futuras.any() else "OK"
    })

    # 2. Columnas clave no nulas
    cols_clave = ["location", "date", "population"]
    mask_nulos = df[cols_clave].isnull().any(axis=1)
    resumen.append({
        "nombre_regla": "Columnas clave no nulas",
        "estado": not mask_nulos.any(),
        "filas_afectadas": int(mask_nulos.sum()),
        "notas": f"Nulos en {cols_clave}" if mask_nulos.any() else "OK"
    })

    # 3. Unicidad location+date
    mask_dupes = df.duplicated(subset=["location", "date"], keep=False)
    resumen.append({
        "nombre_regla": "Unicidad location+date",
        "estado": not mask_dupes.any(),
        "filas_afectadas": int(mask_dupes.sum()),
        "notas": "Duplicados detectados" if mask_dupes.any() else "OK"
    })

    # 4. population > 0
    mask_pop = df["population"] <= 0
    resumen.append({
        "nombre_regla": "population > 0",
        "estado": not mask_pop.any(),
        "filas_afectadas": int(mask_pop.sum()),
        "notas": "Valores <= 0" if mask_pop.any() else "OK"
    })

    # 5. new_cases ≥ 0
    mask_cases = df["new_cases"].fillna(0) < 0
    resumen.append({
        "nombre_regla": "new_cases ≥ 0",
        "estado": not mask_cases.any(),
        "filas_afectadas": int(mask_cases.sum()),
        "notas": "Negativos detectados" if mask_cases.any() else "OK"
    })

    # Resultado global
    passed = all(r["estado"] for r in resumen)

    # DataFrame para mostrar en Dagster
    resumen_df = pd.DataFrame(resumen)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "tabla_resumen": MetadataValue.md(resumen_df.to_markdown(index=False)),
            "total_reglas": len(resumen),
            "reglas_fallidas": [r["nombre_regla"] for r in resumen if not r["estado"]]
        }
    )

@asset
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos originales para métricas:
    - Elimina nulos en new_cases o people_vaccinated
    - Elimina duplicados
    - Filtra a Ecuador y país comparativo
    - Selecciona columnas esenciales
    """
    df = leer_datos.copy()

    # 1. Eliminar filas con nulos en columnas clave
    df = df.dropna(subset=["new_cases", "people_vaccinated"])

    # 2. Eliminar duplicados (mantener la primera ocurrencia)
    df = df.drop_duplicates(subset=["location", "date"], keep="first")

    # 3. Filtrar a Ecuador y país comparativo 
    pais_comparativo = "Colombia"
    df = df[df["location"].isin(["Ecuador", pais_comparativo])]

    # 4. Seleccionar columnas esenciales
    columnas = ["location", "date", "new_cases", "people_vaccinated", "population"]
    df = df[columnas]

    # 5. Asegurar tipos correctos
    df["date"] = pd.to_datetime(df["date"])

    return df

@asset_check(asset="datos_procesados")
def validar_datos_procesados(context, datos_procesados: pd.DataFrame):
    errores = []

    # 1. Verificar nulos
    if datos_procesados.isnull().any().any():
        errores.append("Existen valores nulos en el DataFrame.")

    # 2. Verificar duplicados
    if datos_procesados.duplicated().any():
        errores.append("Existen filas duplicadas en el DataFrame.")

    # 3. Verificar países permitidos
    paises_permitidos = {"Ecuador", "Colombia"}
    if not set(datos_procesados["location"]).issubset(paises_permitidos):
        errores.append(f"Se encontraron países fuera de {paises_permitidos}.")

    # Resultado
    if errores:
        return AssetCheckResult(
            passed=False,
            metadata={"errores": errores, "total_filas": len(datos_procesados)}
        )
    else:
        return AssetCheckResult(
            passed=True,
            metadata={"total_filas": len(datos_procesados)}
        )
    
@asset
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()

    # 1. Calcular incidencia diaria
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100_000

    # 2. Calcular promedio móvil de 7 días por país
    df["incidencia_7d"] = (
        df.groupby("location")["incidencia_diaria"]
          .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    # 3. Seleccionar columnas finales
    resultado = df[["date", "location", "incidencia_7d"]].rename(
        columns={"date": "fecha", "location": "pais"}
    )

    return resultado

from dagster import asset_check, AssetCheckResult
import pandas as pd

@asset_check(asset="metrica_incidencia_7d")
def validar_metrica_incidencia_7d(context, metrica_incidencia_7d: pd.DataFrame):
    # Filtrar valores fuera de rango
    fuera_rango = metrica_incidencia_7d[
        (metrica_incidencia_7d["incidencia_7d"] < 0) |
        (metrica_incidencia_7d["incidencia_7d"] > 2000)
    ]

    passed = fuera_rango.empty

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_registros": MetadataValue.int(len(metrica_incidencia_7d)),
            "fuera_de_rango": MetadataValue.int(len(fuera_rango)),
            "ejemplos_fuera_rango": MetadataValue.md(
                fuera_rango.head().to_markdown(index=False)
            ) if not fuera_rango.empty else MetadataValue.text("Ninguno")
        }
    )


@asset
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()

    # Calcular casos semanales actuales y previos
    df["casos_semana_actual"] = (
        df.groupby("location")["new_cases"]
          .transform(lambda x: x.rolling(7, min_periods=7).sum())
    )
    df["casos_semana_prev"] = df.groupby("location")["casos_semana_actual"].shift(7)

    # Evitar divisiones por bases muy pequeñas
    df.loc[df["casos_semana_prev"] < 10, "casos_semana_prev"] = None

    # Calcular factor
    df["factor_crec_7d"] = df["casos_semana_actual"] / df["casos_semana_prev"]

    # Marcar nulos y outliers
    df["es_nulo_factor"] = df["factor_crec_7d"].isna()
    df["es_outlier_factor"] = df["factor_crec_7d"] > 10

    return df

@asset_check(asset="metrica_factor_crec_7d")
def validar_metrica_factor_crec_7d(context, metrica_factor_crec_7d: pd.DataFrame):
    # Solo detectar outliers > 10
    outliers = int((metrica_factor_crec_7d["factor_crec_7d"] > 10).sum())

    passed = outliers == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "outliers_factor_crec_7d": MetadataValue.int(outliers),
            "nota": MetadataValue.text(
                "Outliers >10 pueden deberse a bases muy pequeñas."
            )
        }
    )


@asset
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> None:
    """
    Exporta los resultados finales a un archivo Excel con tres hojas:
    - Datos procesados
    - Métrica incidencia_7d
    - Métrica factor_crec_7d
    """
    ruta_salida = "reporte_covid.xlsx"

    with pd.ExcelWriter(ruta_salida, engine="xlsxwriter") as writer:
        datos_procesados.to_excel(writer, sheet_name="datos_procesados", index=False)
        metrica_incidencia_7d.to_excel(writer, sheet_name="metrica_incidencia_7d", index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name="metrica_factor_crec_7d", index=False)

    # Mensaje en logs de Dagster
    print(f"Reporte exportado a {ruta_salida}")