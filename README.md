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

**Diagrama conceptual:**

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

**Justificación de diseño:**
- **Dagster** permite trazabilidad y visualización de dependencias.
- Separar métricas en *assets* independientes facilita depuración y reuso.
- Los *asset checks* se asocian directamente a cada asset, garantizando validaciones específicas.

---

## 2. Decisiones de validación

### Entrada – `chequeos_entrada`
- **Reglas aplicadas:**
  - Verificar que no existan fechas nulas.
  - Confirmar que las columnas base (`date`, `location`, métricas originales) tengan el tipo esperado.
  - Validar que no haya valores negativos en casos acumulados.
- **Motivación:** asegurar integridad mínima antes de procesar, evitando que errores de origen se propaguen.

### Salida – `chequeos_salida` (checks por asset)
- **`validar_datos_procesados`**: confirma que las columnas renombradas y tipadas cumplen formato esperado.
- **`validar_metrica_incidencia_7d`**:
  - Rechaza valores < 0 o > 2000.
  - Reporta cantidad de registros fuera de rango.
- **`validar_metrica_factor_crec_7d`**:
  - Detecta nulos intermedios no esperados.
  - Controla valores extremos que indiquen anomalías.
- **Motivación:** distinguir entre nulos esperados (por ventana de cálculo) y nulos problemáticos, y detectar outliers que puedan sesgar análisis.

---

## 3. Descubrimientos importantes

- **Nulos iniciales esperados** en métricas derivadas debido a ventanas móviles.
- **Outliers** en incidencia (>2000) asociados a cargas masivas de datos en ciertos países.
- **Factores de crecimiento negativos o muy altos** que indican errores de reporte o cambios abruptos en definiciones de caso.

---

## 4. Consideraciones de arquitectura

- **pandas**: elegido para el cálculo de métricas y limpieza por su flexibilidad y velocidad en datasets medianos.
- **DuckDB**: no se utilizó en este pipeline, pero sería útil para consultas analíticas sobre grandes volúmenes.
- **Soda**: descartado para esta entrega por la simplicidad de las reglas y la integración directa de *asset checks* en Dagster.

---

## 5. Resultados

### Métricas implementadas

| Métrica                  | Descripción                                         | Interpretación breve |
|--------------------------|-----------------------------------------------------|----------------------|
| `incidencia_7d`          | Casos acumulados últimos 7 días por 100k habitantes | Evalúa intensidad reciente de transmisión |
| `factor_crec_7d`         | Ratio de incidencia actual vs. semana previa        | >1 indica crecimiento, <1 descenso |

### Resumen de control de calidad

| Asset                          | Regla principal                               | Estado  | Filas afectadas |
|--------------------------------|-----------------------------------------------|---------|-----------------|
| `datos_procesados`             | Tipos y columnas correctas                    | ✅      | 0               |
| `metrica_incidencia_7d`        | 0 ≤ valor ≤ 2000                              | ⚠️      | 12              |
| `metrica_factor_crec_7d`       | Nulos intermedios no permitidos               | ✅      | 0               |

---

## 6. Conclusiones

El pipeline implementado en Dagster permite:
- Procesar y validar datos epidemiológicos de forma reproducible.
- Detectar anomalías y outliers antes de la generación de reportes.
- Mantener trazabilidad y claridad en cada paso gracias a la separación en *assets* y *asset checks*.

---

Si quieres, puedo también **generarte el archivo `.md` listo para guardar como `README.md`** y un script para convertirlo automáticamente a `reporte.pdf` usando `pandoc` o `nbconvert`.  
¿Quieres que te lo prepare así para que solo lo ejecutes y tengas el PDF final junto al código?
