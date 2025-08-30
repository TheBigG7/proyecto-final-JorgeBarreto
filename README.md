# Informe Técnico – Pipeline de Métricas COVID-19

## 1. Arquitectura del pipeline

El pipeline se implementa en **Dagster** y está compuesto por un conjunto de *assets* y *asset checks* que procesan datos epidemiológicos, calculan métricas derivadas y generan un reporte en Excel.

**Flujo general:**
1. **Lectura de datos** (`leer_datos`): ingesta desde archivo CSV original.
2. **Procesamiento** (`datos_procesados`): limpieza, normalización de columnas y tipado.
3. **Cálculo de métricas**:
   - `metrica_incidencia_7d`: incidencia acumulada a 7 días.
   - `metrica_factor_crec_7d`: factor de crecimiento semanal.
4. **Generación de reporte** (`reporte_excel_covid`): exporta métricas y validaciones a Excel.
5. **Validaciones** (*asset checks*):
   - `chequeos_entrada`
   - `validar_datos_procesados`
   - `validar_metrica_incidencia_7d`
   - `validar_metrica_factor_crec_7d`

**Diagrama:**

```
[CSV Original] 
     ↓
[leer_datos] 
     ↓
[datos_procesados] → (chequeos_entrada, validar_datos_procesados)
     ↓
 ┌───────────────────────────────┐
 │ metrica_incidencia_7d          │ → validar_metrica_incidencia_7d
 │ metrica_factor_crec_7d         │ → validar_metrica_factor_crec_7d
 └───────────────────────────────┘
     ↓
[reporte_excel_covid]

```


### Assets definidos

- `eda_manual`: Perfilado inicial del CSV manual.
- `leer_datos`: Descarga automática desde OWID.
- `chequeos_entrada`: Validaciones estructurales y semánticas.
- `datos_procesados`: Filtrado por países, selección de columnas, tipos, elimina nulos y duplicados si existen.
- `metrica_incidencia_7d`: Cálculo de incidencia semanal.
- `metrica_factor_crec_7d`: Cálculo de factor de crecimiento.
- `chequeos_salida`: Validaciones sobre métricas.
- `reporte_excel_covid`: Exportación final a Excel.
     `NOTA: Si las fechas en el excel generado se ven asi -> ##### amplie la columna para poder ver el contenido`

## 2. Justificación de Diseño

- **Dagster como orquestador**: Permite visualización de dependencias, control de versiones y validaciones integradas.
- **Uso de `@asset_check`**: Validaciones acopladas a cada asset, con reporte en UI.
- **Separación clara de etapas**: Facilita debugging y reproducibilidad.

## 3. Validaciones Implementadas

### Entrada (`chequeos_entrada`)
- `date ≤ hoy`: Evita fechas futuras.
- `location`, `date`, `population` no nulos.
- `(location, date)` único.
- `population > 0`
- `new_cases ≥ 0` (con tolerancia a negativos documentados)

### Procesamiento (`datos_procesados`)
- Filtrado por países permitidos: `{"Ecuador", "Colombia"}`
- Selección de columnas esenciales: `["location", "date", "new_cases", "people_vaccinated", "population"]`
- Conversión de tipos y fechas
- Validación de nulos y duplicados

### Salida (`chequeos_salida`)
- `incidencia_7d` en rango `[0, 2000]`
- `factor_crec_7d` sin outliers extremos (>10) justificados por bases pequeñas

## 4. Descubrimientos Importantes

- **Outliers en `factor_crec_7d`**: Detectados valores >10, atribuidos a bases poblacionales pequeñas
- **Cobertura desigual**: Colombia tiene más días con datos incompletos.
- **Nulos en vacunación**: Alta proporción en ciertos periodos, especialmente en Ecuador.

## 5. Consideraciones de Arquitectura

### Herramientas Evaluadas

| Herramienta | Uso | Justificación |
|-------------|-----|----------------|
| Pandas      | x  | Flexibilidad para cálculos y filtrado |
| DuckDB      | -  | No necesario para volumen actual |
| Soda        | -  | Se prefirió validación nativa con `@asset_check` |

## 6. Resultados

### Métricas Calculadas

| País     | Fecha       | Incidencia 7d | Factor Crec. 7d |
|----------|-------------|---------------|-----------------|
| Ecuador  | 2021-07-01  | 10.6          | 1.15            |
| Colombia | 2021-07-01  | 15.7          | 0.92            |

### Control de Calidad

| Asset                  | Check                        | Estado | Detalles |
|------------------------|------------------------------|--------|----------|
| `metrica_incidencia_7d` | `validar_metrica_incidencia_7d` | ✅     | 861 registros, sin fuera de rango |
| `metrica_factor_crec_7d` | `validar_metrica_factor_crec_7d` | ✅     | 0 outliers >10, justificados |
| `datos_procesados`     | `validar_datos_procesados`       | ✅     | Duplicados y nulos detectados, corregidos |

---
