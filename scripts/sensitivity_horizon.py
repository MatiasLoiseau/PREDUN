"""
Análisis de sensibilidad del HORIZONTE de abandono y tasa de reactivación.

Responde a las observaciones del jurado sobre por qué el horizonte de inactividad
se fija en 4 períodos (2 años) y cuántos estudiantes clasificados como abandono
vuelven a registrar actividad. Es una justificación EMPÍRICA de una de las
decisiones más importantes de la tesis (definición de la variable objetivo).

Metodología (sin re-entrenar; sobre canonical.cursada_historica, ya delimitada a
las 29 carreras de grado y tecnicatura):

  1. Para cada legajo se construye la línea de períodos con actividad, asignando
     cada cursada a un cuatrimestre del calendario por su FECHA real (idéntico
     criterio que marts/student_panel.sql). Cada período se indexa con un entero
     pidx = anio*2 + bit (bit=0 para 1C/marzo, 1 para 2C/septiembre).

  2. Se identifican los "huecos" de inactividad de cada legajo:
       - hueco INTERNO: entre dos períodos con actividad -> el estudiante RETOMÓ.
       - hueco FINAL (trailing): desde la última actividad hasta el fin de los
         datos -> el estudiante NO retomó dentro de la ventana observada (censura).

  3. Para cada horizonte H ∈ {2,3,4,6}:
       - abandonos (trailing >= H, sin finalización estimada) = legajos que estarían en
         estado de abandono al cierre de los datos bajo ese horizonte.
       - tasa de reactivación = huecos internos >= H / (internos >= H + trailing >= H):
         fracción de las inactividades de al menos H períodos que resultaron
         TEMPORALES. Un H demasiado corto produce una tasa de reactivación alta
         (muchos "falsos abandonos" que en realidad volvieron); el H elegido debe
         estabilizar esa tasa en un valor bajo.

Uso:
    conda run -n eda-predun python scripts/sensitivity_horizon.py
"""
import os
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter4"
)
HORIZONS = [2, 3, 4, 6]


def pidx_from_row(anio, mes):
    """Índice entero de período según la fecha real (igual que student_panel.sql)."""
    if 3 <= mes <= 8:
        return anio * 2 + 0          # 1C (marzo)
    elif mes >= 9:
        return anio * 2 + 1          # 2C (septiembre)
    else:
        return (anio - 1) * 2 + 1    # ene/feb -> 2C del año anterior


def pidx_to_label(p):
    return f"{p // 2}_{'1C' if p % 2 == 0 else '2C'}"


def _set_style():
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        pass
    plt.rcParams.update({
        "font.family": "serif", "font.size": 11, "axes.titlesize": 12,
        "axes.labelsize": 11, "legend.fontsize": 9,
        "savefig.dpi": 300, "savefig.bbox": "tight", "savefig.pad_inches": 0.1,
    })


def main():
    engine = create_engine(PG_URI)

    print("Cargando actividad por legajo desde canonical.cursada_historica...")
    act = pd.read_sql(text("""
        SELECT legajo,
               extract(year  from trim(fecha)::date)::int AS anio,
               extract(month from trim(fecha)::date)::int AS mes
        FROM canonical.cursada_historica
        WHERE trim(coalesce(fecha,'')) ~ '^\\d{4}-\\d{2}-\\d{2}$'
    """), engine)
    act["pidx"] = [pidx_from_row(a, m) for a, m in zip(act["anio"], act["mes"])]

    grad = pd.read_sql(text("""
        SELECT DISTINCT legajo FROM canonical.porcentaje_avance
        WHERE coalesce(
            CASE WHEN trim(porcentaje_avance) ~ '^\\d+([.,]\\d+)?$'
                 THEN replace(trim(porcentaje_avance), ',', '.')::numeric ELSE NULL END, 0) >= 90
    """), engine)
    finalizacion_estimada = set(grad["legajo"].astype(str))

    data_max = int(act["pidx"].max())
    print(f"Período máximo en datos: {pidx_to_label(data_max)} (pidx={data_max})")
    print(f"Legajos con actividad: {act['legajo'].nunique():,} | finalización estimada (avance>=90%): {len(finalizacion_estimada):,}")

    # Por legajo: lista ordenada de períodos con actividad y sus huecos
    internal_gaps = []          # longitudes de huecos internos (estudiante retomó)
    trailing_gaps = {}          # legajo -> longitud del hueco final (censurado)
    for legajo, sub in act.groupby("legajo"):
        ps = np.sort(sub["pidx"].unique())
        # huecos internos
        for a, b in zip(ps[:-1], ps[1:]):
            gap = int(b - a - 1)
            if gap >= 1:
                internal_gaps.append(gap)
        # hueco final
        trailing_gaps[str(legajo)] = int(data_max - ps[-1])

    internal_gaps = np.array(internal_gaps)
    trailing = pd.Series(trailing_gaps)

    rows = []
    for H in HORIZONS:
        n_internal_geH = int((internal_gaps >= H).sum())
        # abandono al cierre: trailing >= H y sin finalización estimada
        trailing_legajos = trailing[trailing >= H].index
        n_trailing_geH = len(trailing_legajos)
        n_abandono = sum(1 for lg in trailing_legajos if lg not in finalizacion_estimada)
        n_finaliz_excl = n_trailing_geH - n_abandono
        denom = n_internal_geH + n_trailing_geH
        reactivacion = n_internal_geH / denom if denom else float("nan")
        rows.append(dict(
            horizonte_periodos=H,
            anios=H / 2,
            abandono_legajos=n_abandono,
            finalizacion_excluida=n_finaliz_excl,
            inactividades_internas_geH=n_internal_geH,
            inactividades_finales_geH=n_trailing_geH,
            tasa_reactivacion=round(reactivacion, 4),
        ))

    res = pd.DataFrame(rows)
    pd.set_option("display.width", 200, "display.max_columns", 30)
    print("\n=== Sensibilidad al horizonte de abandono ===")
    print(res.to_string(index=False))
    print("\nLectura: 'tasa_reactivacion' = fracción de inactividades de >= H períodos "
          "que resultaron temporales\n(el estudiante volvió). Un horizonte adecuado la "
          "estabiliza en un valor bajo.")

    # Figura: tasa de reactivación y nº de abandonos por horizonte
    _set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)
    fig, ax1 = plt.subplots(figsize=(7, 4.8))
    x = res["horizonte_periodos"].values
    ax1.plot(x, res["tasa_reactivacion"] * 100, "o-", color="#B71C1C", lw=2,
             markersize=8, label="Tasa de reactivación (%)")
    ax1.set_xlabel("Horizonte de inactividad (períodos)")
    ax1.set_ylabel("Tasa de reactivación posterior (%)", color="#B71C1C")
    ax1.tick_params(axis="y", labelcolor="#B71C1C")
    ax1.set_xticks(x)
    for xi, v in zip(x, res["tasa_reactivacion"] * 100):
        ax1.text(xi, v + 0.6, f"{v:.1f}%", ha="center", fontsize=9, color="#B71C1C", fontweight="bold")
    ax2 = ax1.twinx()
    ax2.bar(x, res["abandono_legajos"], width=0.35, alpha=0.25, color="#1565C0",
            label="Legajos en abandono")
    ax2.set_ylabel("Legajos clasificados como abandono", color="#1565C0")
    ax2.tick_params(axis="y", labelcolor="#1565C0")
    ax1.axvline(4, color="gray", ls=":", lw=1.2)
    ax1.set_title("Sensibilidad de la definición de abandono al horizonte de inactividad")
    fig.tight_layout()
    path = os.path.join(THESIS_FIGS_DIR, "sensitivity_horizon.png")
    fig.savefig(path); plt.close(fig)
    print(f"\nGuardado: {path}")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.sensitivity_horizon"))
        conn.commit()
    res.to_sql("sensitivity_horizon", engine, schema="predictions", if_exists="replace", index=False)
    print("Resultados persistidos en predictions.sensitivity_horizon")


if __name__ == "__main__":
    main()
