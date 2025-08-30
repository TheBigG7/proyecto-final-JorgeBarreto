"""
EDA manual inicial para compact.csv
Genera un perfilado básico y guarda tabla_perfilado.csv
"""

import pandas as pd
from pathlib import Path

# Ruta dinámica al CSV original
RUTA_CSV = Path(__file__).parent / "datos_originales" / "compact.csv"
RUTA_PERFILADO = Path(__file__).parent / "tabla_perfilado.csv"

def main():
    # 1. Cargar datos
    df = pd.read_csv(RUTA_CSV)

    # 2. Info básica
    print("\n=== INFO DEL DATAFRAME ===")
    print(df.info())
    print("\n=== PRIMERAS FILAS ===")
    print(df.head())

    # 3. Perfilado
    perfilado = pd.DataFrame({
        "columna": df.columns,
        "tipo_dato": df.dtypes.astype(str),
        "nulos": df.isnull().sum(),
        "nulos_%": (df.isnull().sum() / len(df) * 100).round(2)
    })

    # 4. Guardar perfilado
    perfilado.to_csv(RUTA_PERFILADO, index=False)
    print(f"\nPerfilado guardado en: {RUTA_PERFILADO}")

    # 5. Ejemplo de métricas rápidas
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            print(f"Rango de fechas: {df['date'].min()} → {df['date'].max()}")
        except Exception as e:
            print(f"No se pudo convertir 'date' a datetime: {e}")

if __name__ == "__main__":
    main()
