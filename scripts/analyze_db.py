"""
Análisis de texto directo desde PostgreSQL — PREDUN.
Genera un reporte en texto sin imágenes para inspección rápida del estado de la BD.
Pensado para ejecutarse después de cada ciclo de DAGs y comparar entre ciclos.

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/analyze_db.py
    conda run -n eda-predun python scripts/analyze_db.py --cycle 2024_2C
"""

import argparse
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
# Corte de entrenamiento con embargo de maduración (VAL_PERIOD - 4) del ciclo
# final; aquí se usa solo para etiquetar descriptivamente train vs. post-train.
TRAIN_CUTOFF = "2021_1C"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__))


def separator(char="=", width=70):
    return char * width


def section(title):
    print(f"\n{separator()}")
    print(f"  {title}")
    print(separator())


def hr():
    print(separator("-", 70))


def check_table(engine, schema, table):
    try:
        n = pd.read_sql(
            f"SELECT COUNT(*) AS n FROM {schema}.{table}", engine
        ).iloc[0]["n"]
        return int(n)
    except Exception:
        return -1


# ─────────────────────────────────────────────────────────────────────────────
# 1. RESUMEN DE INGESTA (staging)
# ─────────────────────────────────────────────────────────────────────────────
def analyze_staging(engine):
    section("1. STAGING — Datos ingestados")

    tables = {
        "cursada_historica_raw": "staging",
        "alumnos_raw":           "staging",
        "porcentaje_avance_raw": "staging",
    }

    for table, schema in tables.items():
        try:
            df = pd.read_sql(
                f"SELECT academic_period, COUNT(*) AS n "
                f"FROM {schema}.{table} "
                f"GROUP BY academic_period ORDER BY academic_period",
                engine,
            )
            total = df["n"].sum()
            print(f"\n  {schema}.{table}  (total: {total:,} filas)")
            if df.empty:
                print("    (vacía)")
            else:
                for _, row in df.iterrows():
                    print(f"    {row['academic_period']:12s}  {row['n']:>8,} filas")
        except Exception as e:
            print(f"  {schema}.{table}: ERROR — {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. CANONICAL — Calidad de datos
# ─────────────────────────────────────────────────────────────────────────────
def analyze_canonical(engine):
    section("2. CANONICAL — Calidad de datos")

    # cursada_historica (columna de fecha: 'fecha', no 'fecha_inicio_periodo')
    try:
        df = pd.read_sql(
            """
            SELECT
                COUNT(*) AS total_filas,
                COUNT(DISTINCT legajo) AS legajos_unicos,
                COUNT(DISTINCT cod_carrera) AS carreras,
                MIN(academic_period) AS primer_periodo,
                MAX(academic_period) AS ultimo_periodo,
                SUM(CASE WHEN nota IS NULL OR nota = '' THEN 1 ELSE 0 END) AS nulos_nota
            FROM canonical.cursada_historica
            """,
            engine,
        )
        r = df.iloc[0]
        print("\n  canonical.cursada_historica")
        print(f"    Filas totales      : {int(r['total_filas']):,}")
        print(f"    Legajos únicos     : {int(r['legajos_unicos']):,}")
        print(f"    Carreras           : {int(r['carreras']):,}")
        print(f"    Primer período     : {r['primer_periodo']}")
        print(f"    Último período     : {r['ultimo_periodo']}")
        print(f"    Nulos en 'nota'    : {int(r['nulos_nota']):,}")
    except Exception as e:
        print(f"  canonical.cursada_historica: ERROR — {e}")

    # alumnos (columna de género: 'sexo', no 'genero')
    try:
        df = pd.read_sql(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT legajo) AS legajos_unicos,
                SUM(CASE WHEN sexo IS NULL OR sexo = '' THEN 1 ELSE 0 END) AS nulos_sexo,
                SUM(CASE WHEN fecha_nacimiento IS NULL OR fecha_nacimiento = '' THEN 1 ELSE 0 END) AS nulos_fnac
            FROM canonical.alumnos
            """,
            engine,
        )
        r = df.iloc[0]
        print("\n  canonical.alumnos")
        print(f"    Filas totales      : {int(r['total']):,}")
        print(f"    Legajos únicos     : {int(r['legajos_unicos']):,}")
        print(f"    Nulos en 'sexo'    : {int(r['nulos_sexo']):,}")
        print(f"    Nulos en 'fecha_nacimiento': {int(r['nulos_fnac']):,}")
    except Exception as e:
        print(f"  canonical.alumnos: ERROR — {e}")

    # porcentaje_avance — análisis crítico de la finalización curricular estimada
    # porcentaje_avance está almacenado como TEXT — requiere CAST con reemplazo de coma decimal
    try:
        df = pd.read_sql(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT legajo) AS legajos_unicos,
                AVG(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC)) AS promedio_avance,
                MIN(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC)) AS minimo,
                MAX(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC)) AS maximo,
                SUM(CASE WHEN CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC) >= 90 THEN 1 ELSE 0 END) AS sobre_90,
                SUM(CASE WHEN CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC) >= 100 THEN 1 ELSE 0 END) AS sobre_100,
                SUM(CASE WHEN porcentaje_avance IS NULL OR porcentaje_avance = '' THEN 1 ELSE 0 END) AS nulos
            FROM canonical.porcentaje_avance
            WHERE porcentaje_avance ~ '^[0-9,\\.]+$'
            """,
            engine,
        )
        r = df.iloc[0]
        total = int(r["total"])
        sobre_90  = int(r["sobre_90"])
        sobre_100 = int(r["sobre_100"])
        print("\n  canonical.porcentaje_avance  *** área de la finalización curricular estimada ***")
        print(f"    Filas con valor numérico : {total:,}")
        print(f"    Legajos únicos           : {int(r['legajos_unicos']):,}")
        print(f"    Promedio avance          : {r['promedio_avance']:.2f}%")
        print(f"    Rango                    : [{r['minimo']:.1f}% — {r['maximo']:.1f}%]")
        print(f"    Con avance >= 90%        : {sobre_90:,}  ({sobre_90/total*100:.1f}%)")
        print(f"    Con avance = 100%        : {sobre_100:,}  ({sobre_100/total*100:.1f}%)")

        # Distribución por decil
        deciles = pd.read_sql(
            """
            SELECT
                FLOOR(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC) / 10) * 10 AS decil_inicio,
                COUNT(*) AS n
            FROM canonical.porcentaje_avance
            WHERE porcentaje_avance ~ '^[0-9,\\.]+$'
            GROUP BY decil_inicio
            ORDER BY decil_inicio
            """,
            engine,
        )
        print("\n    Distribución por decil de avance:")
        for _, row in deciles.iterrows():
            barra = "█" * int(row["n"] / deciles["n"].max() * 30)
            print(f"    [{int(row['decil_inicio']):3d}%-{int(row['decil_inicio'])+9:3d}%]  {barra}  {int(row['n']):,}")

    except Exception as e:
        print(f"  canonical.porcentaje_avance: ERROR — {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. STUDENT STATUS — distribución de estados (donde estaba el error)
# ─────────────────────────────────────────────────────────────────────────────
def analyze_student_status(engine):
    section("3. MARTS.STUDENT_STATUS — Distribución de estados (zona del error)")

    try:
        df = pd.read_sql(
            """
            SELECT
                status,
                COUNT(*) AS n,
                COUNT(DISTINCT legajo) AS legajos_unicos
            FROM marts.student_status
            GROUP BY status
            ORDER BY n DESC
            """,
            engine,
        )
        if df.empty:
            print("  (tabla vacía)")
            return

        total = df["n"].sum()
        print(f"\n  Total legajos en student_status: {total:,}")
        print()
        for _, row in df.iterrows():
            pct = row["n"] / total * 100
            barra = "█" * int(pct / 2)
            print(f"  {row['status']:30s}  {int(row['n']):>8,}  ({pct:5.1f}%)  {barra}")

        # Detalle de finalización curricular estimada por carrera
        print("\n  Finalización curricular estimada por carrera (top 15 por volumen):")
        hr()
        grad = pd.read_sql(
            """
            SELECT
                pa.cod_carrera,
                COUNT(DISTINCT ss.legajo) AS n_finalizacion,
                AVG(CAST(REPLACE(pa.porcentaje_avance, ',', '.') AS NUMERIC)) AS avance_promedio
            FROM marts.student_status ss
            LEFT JOIN canonical.porcentaje_avance pa USING (legajo)
            WHERE ss.status = 'finalizacion_estimada'
              AND (pa.porcentaje_avance IS NULL OR pa.porcentaje_avance ~ '^[0-9,\\.]+$')
            GROUP BY pa.cod_carrera
            ORDER BY n_finalizacion DESC
            LIMIT 15
            """,
            engine,
        )
        if not grad.empty:
            print(f"  {'Carrera':<20} {'Finaliz.est':>12} {'Avance prom':>12}")
            print(f"  {'-'*20} {'-'*12} {'-'*12}")
            for _, row in grad.iterrows():
                carrera = str(row["cod_carrera"] or "N/A")
                avg_av = f"{row['avance_promedio']:.1f}%" if pd.notna(row["avance_promedio"]) else "N/A"
                print(f"  {carrera:<20} {int(row['n_finalizacion']):>12,} {avg_av:>12}")
        else:
            print("  (sin finalización estimada o sin join posible)")

        # Alumnos con avance >= 90% clasificados como SIN finalización estimada (potencial error)
        print("\n  Legajos con avance >= 90% pero NO clasificados como finalizacion_estimada:")
        hr()
        try:
            anomaly = pd.read_sql(
                """
                SELECT COUNT(*) AS n
                FROM marts.student_status ss
                JOIN canonical.porcentaje_avance pa USING (legajo)
                WHERE pa.porcentaje_avance ~ '^[0-9,\\.]+$'
                  AND CAST(REPLACE(pa.porcentaje_avance, ',', '.') AS NUMERIC) >= 90
                  AND ss.status != 'finalizacion_estimada'
                """,
                engine,
            )
            n_anom = int(anomaly.iloc[0]["n"])
            print(f"  {n_anom:,} legajos con avance >= 90% pero status != 'finalizacion_estimada'")
            if n_anom > 0:
                detail = pd.read_sql(
                    """
                    SELECT ss.status, COUNT(*) AS n
                    FROM marts.student_status ss
                    JOIN canonical.porcentaje_avance pa USING (legajo)
                    WHERE pa.porcentaje_avance ~ '^[0-9,\\.]+$'
                      AND CAST(REPLACE(pa.porcentaje_avance, ',', '.') AS NUMERIC) >= 90
                      AND ss.status != 'finalizacion_estimada'
                    GROUP BY ss.status
                    ORDER BY n DESC
                    """,
                    engine,
                )
                for _, row in detail.iterrows():
                    print(f"    status='{row['status']}': {int(row['n']):,}")
        except Exception as e:
            print(f"  No se pudo calcular anomalía: {e}")

    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. STUDENT PANEL — composición del panel de entrenamiento
# ─────────────────────────────────────────────────────────────────────────────
def analyze_student_panel(engine):
    section("4. MARTS.STUDENT_PANEL — Panel de entrenamiento")

    try:
        # Composición global del panel: en riesgo / fuera de riesgo / censura
        composicion = pd.read_sql(
            """
            SELECT
                COUNT(*)                                                     AS total_filas,
                SUM(at_risk)                                                 AS filas_at_risk,
                SUM(CASE WHEN at_risk = 1 AND dropout_next IS NOT NULL
                         THEN 1 ELSE 0 END)                                  AS filas_modelado,
                SUM(CASE WHEN dropout_next IS NULL THEN 1 ELSE 0 END)        AS filas_censuradas
            FROM marts.student_panel
            """,
            engine,
        )
        c = composicion.iloc[0]
        print(f"\n  Filas totales panel : {int(c['total_filas']):,}")
        print(f"  Filas at_risk=1     : {int(c['filas_at_risk']):,}")
        print(f"  Filas de modelado   : {int(c['filas_modelado']):,}  (at_risk=1 y etiqueta observable)")
        print(f"  Filas censuradas    : {int(c['filas_censuradas']):,}  (ventana futura insuficiente)")

        overview = pd.read_sql(
            """
            SELECT
                COUNT(*) AS total_filas,
                COUNT(DISTINCT legajo) AS legajos_unicos,
                COUNT(DISTINCT cod_carrera) AS carreras,
                COUNT(DISTINCT academic_period) AS periodos,
                AVG(dropout_next) AS tasa_dropout_global,
                SUM(CASE WHEN dropout_next = 1 THEN 1 ELSE 0 END) AS filas_abandono,
                SUM(CASE WHEN dropout_next = 0 THEN 1 ELSE 0 END) AS filas_activo
            FROM marts.student_panel
            WHERE at_risk = 1 AND dropout_next IS NOT NULL
            """,
            engine,
        )
        r = overview.iloc[0]
        total = int(r["total_filas"])
        print(f"\n  — Población de modelado (at_risk=1, etiquetada) —")
        print(f"  Filas               : {total:,}")
        print(f"  Legajos únicos      : {int(r['legajos_unicos']):,}")
        print(f"  Carreras            : {int(r['carreras']):,}")
        print(f"  Períodos cubiertos  : {int(r['periodos']):,}")
        print(f"  Tasa abandono global: {r['tasa_dropout_global']*100:.1f}%")
        print(f"  Filas dropout=1     : {int(r['filas_abandono']):,}  ({int(r['filas_abandono'])/total*100:.1f}%)")
        print(f"  Filas dropout=0     : {int(r['filas_activo']):,}  ({int(r['filas_activo'])/total*100:.1f}%)")

        # Split train/val
        print("\n  División train / validación (corte: 2022_2C):")
        hr()
        split = pd.read_sql(
            f"""
            SELECT
                CASE WHEN academic_period <= '{TRAIN_CUTOFF}' THEN 'train' ELSE 'val' END AS split,
                COUNT(*) AS n,
                AVG(dropout_next) AS tasa_dropout
            FROM marts.student_panel
            WHERE at_risk = 1 AND dropout_next IS NOT NULL
            GROUP BY split
            ORDER BY split
            """,
            engine,
        )
        for _, row in split.iterrows():
            print(f"  {row['split']:6s}  {int(row['n']):>10,} filas  dropout={row['tasa_dropout']*100:.1f}%")

        # Por período
        print("\n  Filas y tasa de dropout por período académico (población de modelado):")
        hr()
        by_period = pd.read_sql(
            """
            SELECT
                academic_period,
                COUNT(*) AS n,
                COUNT(DISTINCT legajo) AS legajos,
                AVG(dropout_next) AS tasa_dropout
            FROM marts.student_panel
            WHERE at_risk = 1 AND dropout_next IS NOT NULL
            GROUP BY academic_period
            ORDER BY academic_period
            """,
            engine,
        )
        print(f"  {'Período':<12} {'Filas':>8} {'Legajos':>8} {'Dropout':>8}  Split")
        print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8}  -----")
        for _, row in by_period.iterrows():
            split = "TRAIN" if row["academic_period"] <= TRAIN_CUTOFF else "val  "
            print(f"  {row['academic_period']:<12} {int(row['n']):>8,} {int(row['legajos']):>8,} {row['tasa_dropout']*100:>7.1f}%  {split}")

    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. PREDICCIONES — análisis de la cohorte activa
# ─────────────────────────────────────────────────────────────────────────────
def analyze_predictions(engine):
    section("5. PREDICTIONS — Cohorte activa con riesgo predicho")

    try:
        n = check_table(engine, "predictions", "student_dropout_predictions")
        if n <= 0:
            print("  (tabla vacía o no existe — ejecutar scoring primero)")
            return

        df = pd.read_sql(
            """
            SELECT
                dropout_probability,
                dropout_prediction,
                model_version,
                academic_period,
                prediction_date
            FROM predictions.student_dropout_predictions
            """,
            engine,
        )

        proba = df["dropout_probability"]
        print(f"\n  Total predicciones   : {len(df):,}")
        print(f"  Modelo versión       : {df['model_version'].iloc[0] if len(df) > 0 else 'N/A'}")
        print(f"  Período de datos     : {df['academic_period'].iloc[0] if len(df) > 0 else 'N/A'}")
        print(f"  Fecha predicción     : {df['prediction_date'].iloc[0] if len(df) > 0 else 'N/A'}")

        n_total = len(df)
        n_high = (proba >= 0.7).sum()
        n_med  = ((proba >= 0.5) & (proba < 0.7)).sum()
        n_low  = (proba < 0.5).sum()

        print(f"\n  Distribución de riesgo:")
        print(f"    Riesgo alto  (>= 0.70) : {n_high:>6,}  ({n_high/n_total*100:.1f}%)")
        print(f"    Riesgo medio [0.5-0.7) : {n_med:>6,}  ({n_med/n_total*100:.1f}%)")
        print(f"    Riesgo bajo  (<  0.50) : {n_low:>6,}  ({n_low/n_total*100:.1f}%)")

        print(f"\n  Estadísticas de probabilidad:")
        print(f"    Media    : {proba.mean():.4f}")
        print(f"    Mediana  : {proba.median():.4f}")
        print(f"    Std      : {proba.std():.4f}")
        print(f"    p10      : {proba.quantile(0.10):.4f}")
        print(f"    p25      : {proba.quantile(0.25):.4f}")
        print(f"    p75      : {proba.quantile(0.75):.4f}")
        print(f"    p90      : {proba.quantile(0.90):.4f}")

        # Por carrera
        try:
            by_carrera = pd.read_sql(
                """
                SELECT
                    p.cod_carrera,
                    COUNT(*) AS n,
                    AVG(dropout_probability) AS riesgo_promedio,
                    SUM(CASE WHEN dropout_probability >= 0.7 THEN 1 ELSE 0 END) AS alto_riesgo
                FROM predictions.student_dropout_predictions p
                GROUP BY p.cod_carrera
                ORDER BY riesgo_promedio DESC
                """,
                engine,
            )
            if not by_carrera.empty:
                print(f"\n  Riesgo promedio por carrera (ordenado de mayor a menor):")
                hr()
                print(f"  {'Carrera':<20} {'N est.':>7} {'Riesgo prom.':>13} {'Alto riesgo':>12}")
                print(f"  {'-'*20} {'-'*7} {'-'*13} {'-'*12}")
                for _, row in by_carrera.iterrows():
                    car = str(row.get("cod_carrera", "N/A") or "N/A")
                    n_c = int(row["n"])
                    alto = int(row["alto_riesgo"])
                    print(f"  {car:<20} {n_c:>7,} {row['riesgo_promedio']:>12.4f} {alto:>11,}  ({alto/n_c*100:.0f}%)")
        except Exception:
            pass

    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 6. NUEVAS MÉTRICAS — Análisis de la finalización curricular estimada
# ─────────────────────────────────────────────────────────────────────────────
def analyze_graduate_metrics(engine):
    section("6. ANÁLISIS DE FINALIZACIÓN CURRICULAR ESTIMADA — Métricas nuevas")

    try:
        # porcentaje_avance es TEXT — requiere CAST con REPLACE de coma decimal
        df_g = pd.read_sql(
            """
            WITH avance_max AS (
                SELECT legajo,
                       MAX(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC)) AS avance_max
                FROM canonical.porcentaje_avance
                WHERE porcentaje_avance ~ '^[0-9,\\.]+$'
                GROUP BY legajo
            )
            SELECT
                s.status,
                COUNT(*) AS n,
                ROUND(AVG(a.avance_max)::numeric, 2) AS avance_promedio,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a.avance_max)::numeric, 2) AS avance_mediana,
                MIN(a.avance_max) AS avance_min,
                MAX(a.avance_max) AS avance_max_val,
                SUM(CASE WHEN a.avance_max IS NULL THEN 1 ELSE 0 END) AS sin_avance
            FROM marts.student_status s
            LEFT JOIN avance_max a USING (legajo)
            GROUP BY s.status
            ORDER BY n DESC
            """,
            engine,
        )

        print("\n  Distribución de avance máximo por estado:")
        hr()
        print(f"  {'Status':<15} {'N':>8} {'Avance prom':>12} {'Mediana':>8} {'Min':>6} {'Max':>6} {'Sin avance':>11}")
        print(f"  {'-'*15} {'-'*8} {'-'*12} {'-'*8} {'-'*6} {'-'*6} {'-'*11}")
        for _, row in df_g.iterrows():
            avg_a = f"{row['avance_promedio']:.1f}%" if pd.notna(row["avance_promedio"]) else "N/A"
            med_a = f"{row['avance_mediana']:.1f}%" if pd.notna(row["avance_mediana"]) else "N/A"
            min_a = f"{row['avance_min']:.1f}%" if pd.notna(row["avance_min"]) else "N/A"
            max_a = f"{row['avance_max_val']:.1f}%" if pd.notna(row["avance_max_val"]) else "N/A"
            print(f"  {str(row['status']):<15} {int(row['n']):>8,} {avg_a:>12} {med_a:>8} {min_a:>6} {max_a:>6} {int(row['sin_avance']):>11,}")

        # Umbral de finalización estimada: cuántos quedan fuera con distintos umbrales
        print("\n  Sensibilidad del umbral de finalización curricular estimada:")
        hr()
        for umbral in [85, 88, 90, 92, 95, 100]:
            r = pd.read_sql(
                f"""
                SELECT COUNT(DISTINCT legajo) AS n
                FROM canonical.porcentaje_avance
                WHERE porcentaje_avance ~ '^[0-9,\\.]+$'
                  AND CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC) >= {umbral}
                """,
                engine,
            ).iloc[0]["n"]
            print(f"    Umbral >= {umbral}%  :  {int(r):,} legajos con finalización estimada")

        # Finalización estimada sin actividad reciente en cursada (contraste con egreso real)
        print("\n  Finalización estimada (avance >= 90%) — análisis de última actividad en cursada:")
        hr()
        grad_activity = pd.read_sql(
            """
            WITH avance_max AS (
                SELECT legajo,
                       MAX(CAST(REPLACE(porcentaje_avance, ',', '.') AS NUMERIC)) AS avance_max
                FROM canonical.porcentaje_avance
                WHERE porcentaje_avance ~ '^[0-9,\\.]+$'
                GROUP BY legajo
            ),
            ultima_cursada AS (
                SELECT legajo, MAX(academic_period) AS ultimo_periodo
                FROM canonical.cursada_historica
                GROUP BY legajo
            )
            SELECT
                CASE
                    WHEN uc.ultimo_periodo IS NULL THEN 'Sin cursadas registradas'
                    WHEN uc.ultimo_periodo < '2020_1C' THEN 'Último período < 2020'
                    WHEN uc.ultimo_periodo < '2022_1C' THEN 'Último período 2020-2021'
                    WHEN uc.ultimo_periodo < '2024_1C' THEN 'Último período 2022-2023'
                    ELSE 'Último período >= 2024'
                END AS actividad,
                COUNT(*) AS n
            FROM avance_max am
            LEFT JOIN ultima_cursada uc USING (legajo)
            WHERE am.avance_max >= 90
            GROUP BY actividad
            ORDER BY n DESC
            """,
            engine,
        )
        if not grad_activity.empty:
            total_grad = grad_activity["n"].sum()
            for _, row in grad_activity.iterrows():
                pct = row["n"] / total_grad * 100
                print(f"    {str(row['actividad']):<35}  {int(row['n']):>6,}  ({pct:.1f}%)")

    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 7. MÉTRICAS ADICIONALES DE INTERÉS PARA LA TESIS
# ─────────────────────────────────────────────────────────────────────────────
def analyze_cohort_retention(engine):
    section("7. MÉTRICAS DE RETENCIÓN — Cohortes por año de inscripción")

    try:
        # Usar canonical.alumnos.anio_academico como año de cohorte
        # (más confiable que el min del panel, que es calendar-based)
        cohort = pd.read_sql(
            """
            SELECT
                a.anio_academico AS cohorte,
                ss.status,
                COUNT(DISTINCT ss.legajo) AS n
            FROM marts.student_status ss
            JOIN canonical.alumnos a USING (legajo)
            WHERE a.anio_academico IS NOT NULL
              AND a.anio_academico != ''
              AND a.anio_academico >= '2015'
            GROUP BY a.anio_academico, ss.status
            ORDER BY a.anio_academico, ss.status
            """,
            engine,
        )

        if cohort.empty:
            print("  (sin datos)")
            return

        pivot = cohort.pivot_table(
            index="cohorte", columns="status", values="n", fill_value=0
        )
        pivot["total"] = pivot.sum(axis=1)
        for col in ["abandonó", "estudiando", "finalizacion_estimada"]:
            if col in pivot.columns:
                pivot[f"pct_{col}"] = pivot[col] / pivot["total"] * 100

        print(f"\n  {'Cohorte':<12} {'Total':>8} {'Abandono%':>10} {'Cursando%':>10} {'Finaliz.est%':>10}")
        print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
        for cohorte, row in pivot.iterrows():
            pct_ab = f"{row.get('pct_abandonó', 0):.1f}%"
            pct_cu = f"{row.get('pct_estudiando', 0):.1f}%"
            pct_gr = f"{row.get('pct_finalizacion_estimada', 0):.1f}%"
            print(f"  {cohorte:<12} {int(row['total']):>8,} {pct_ab:>10} {pct_cu:>10} {pct_gr:>10}")

    except Exception as e:
        print(f"  ERROR: {e}")


def analyze_gender_dropout(engine):
    section("8. DROPOUT POR SEXO — Variable demográfica clave")

    try:
        # En canonical.alumnos la columna es 'sexo', no 'genero'
        df = pd.read_sql(
            """
            SELECT
                a.sexo,
                ss.status,
                COUNT(*) AS n
            FROM marts.student_status ss
            JOIN canonical.alumnos a USING (legajo)
            WHERE a.sexo IS NOT NULL AND a.sexo != ''
            GROUP BY a.sexo, ss.status
            ORDER BY a.sexo, ss.status
            """,
            engine,
        )

        if df.empty:
            print("  (sin datos o columna sexo no disponible)")
            return

        pivot = df.pivot_table(
            index="sexo", columns="status", values="n", fill_value=0
        )
        pivot["total"] = pivot.sum(axis=1)
        for col in ["abandonó", "estudiando", "finalizacion_estimada"]:
            if col in pivot.columns:
                pivot[f"pct_{col}"] = pivot[col] / pivot["total"] * 100

        print(f"\n  {'Sexo':<10} {'Total':>8} {'Abandono%':>10} {'Cursando%':>10} {'Finaliz.est%':>10}")
        print(f"  {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
        for sexo, row in pivot.iterrows():
            pct_ab = f"{row.get('pct_abandonó', 0):.1f}%"
            pct_cu = f"{row.get('pct_estudiando', 0):.1f}%"
            pct_gr = f"{row.get('pct_finalizacion_estimada', 0):.1f}%"
            print(f"  {str(sexo):<10} {int(row['total']):>8,} {pct_ab:>10} {pct_cu:>10} {pct_gr:>10}")

        # Identidad de género (campo separado)
        ig = pd.read_sql(
            """
            SELECT identidad_genero, COUNT(*) AS n
            FROM canonical.alumnos
            WHERE identidad_genero IS NOT NULL AND identidad_genero != ''
            GROUP BY identidad_genero
            ORDER BY n DESC
            LIMIT 10
            """,
            engine,
        )
        if not ig.empty:
            total_ig = ig["n"].sum()
            print(f"\n  Identidad de género registrada (top 10):")
            for _, row in ig.iterrows():
                print(f"    {str(row['identidad_genero']):<30}  {int(row['n']):,}  ({row['n']/total_ig*100:.1f}%)")

    except Exception as e:
        print(f"  ERROR: {e}")


def analyze_feature_stats(engine):
    section("9. ESTADÍSTICAS DE FEATURES — student_panel (set validación)")

    try:
        df = pd.read_sql(
            f"""
            SELECT
                academic_period,
                materias_en_periodo, promo_en_periodo, nota_media_en_periodo,
                materias_win3, promo_win3, nota_win3,
                dropout_next
            FROM marts.student_panel
            WHERE academic_period > '{TRAIN_CUTOFF}'
              AND at_risk = 1 AND dropout_next IS NOT NULL
            """,
            engine,
        )

        if df.empty:
            print("  (sin datos en validación)")
            return

        num_cols = [
            "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
            "materias_win3", "promo_win3", "nota_win3",
        ]

        print(f"\n  Set de validación: {len(df):,} filas\n")
        print(f"  {'Feature':<30} {'Media':>8} {'Mediana':>8} {'Std':>8} {'p90':>8}  {'Corr(dropout)':>14}")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8}  {'-'*14}")

        for col in num_cols:
            if col not in df.columns:
                continue
            s = pd.to_numeric(df[col], errors="coerce")
            corr = s.corr(df["dropout_next"])
            print(
                f"  {col:<30} {s.mean():>8.3f} {s.median():>8.3f} {s.std():>8.3f} "
                f"{s.quantile(0.9):>8.3f}  {corr:>14.4f}"
            )

    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(cycle: str | None = None):
    engine = create_engine(PG_URI)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(separator())
    print(f"  PREDUN — ANÁLISIS DE BASE DE DATOS")
    print(f"  Generado: {ts}")
    if cycle:
        print(f"  Ciclo DAG: {cycle}")
    print(separator())

    # Verificación rápida de tablas clave
    print("\n  Verificación de tablas:")
    tables_check = [
        ("staging",     "cursada_historica_raw"),
        ("canonical",   "cursada_historica"),
        ("canonical",   "porcentaje_avance"),
        ("marts",       "student_status"),
        ("marts",       "student_panel"),
        ("predictions", "student_dropout_predictions"),
    ]
    for schema, table in tables_check:
        n = check_table(engine, schema, table)
        status = f"{n:,} filas" if n >= 0 else "NO EXISTE / ERROR"
        print(f"    {schema}.{table:<35} {status}")

    analyze_staging(engine)
    analyze_canonical(engine)
    analyze_student_status(engine)
    analyze_student_panel(engine)
    analyze_predictions(engine)
    analyze_graduate_metrics(engine)
    analyze_cohort_retention(engine)
    analyze_gender_dropout(engine)
    analyze_feature_stats(engine)

    print(f"\n{separator()}")
    print(f"  Análisis completo — {ts}")
    print(separator())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cycle",
        default=None,
        help="Identificador del ciclo DAG ejecutado (ej: 2024_2C)",
    )
    args = parser.parse_args()
    main(cycle=args.cycle)
