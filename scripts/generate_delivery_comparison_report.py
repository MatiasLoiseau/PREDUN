"""
Reconciliación entre entregas institucionales (2024_2C, 2025_1C, 2025_2C).

Segundo eje del monitoreo de datos, complementario del PSI intra-entrega que
calcula el activo `detect_data_drift`. Aquel compara dos tramos de UNA MISMA foto
(referencia de entrenamiento vs. tramo posterior al corte). Éste compara cada
entrega contra la anterior sobre el tramo de historia que ambas dan por cerrado,
donde el valor esperado del PSI no es un umbral heurístico sino CERO EXACTO.

Reconstruye el panel de cada entrega desde las tablas archivadas por el pre_hook
`archive_canonical_table` (canonical.*_history), replicando la lógica de
predun_dbt/models/marts/student_panel.sql, y produce:

  - Tabla 5.12  PSI entre entregas en dos ventanas (reconciliación / completa)
  - Tabla 5.13  Perfil de la frontera de cada entrega
  - Tabla 5.14  Efecto de la frontera vacía sobre el nivel del scoring
  - Conciliación por conteo de eventos, con clave estable y con evento_hash
  - Figura      reconciliacion_cobertura.png  (cobertura por período y entrega)
  - Figura      reconciliacion_scoring.png    (efecto de la frontera vacía)

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_reconciliation_report.py
    conda run -n eda-predun python scripts/generate_reconciliation_report.py --rebuild
    conda run -n eda-predun python scripts/generate_reconciliation_report.py --no-figure

La reconstrucción de los paneles tarda unos minutos. Se hace una sola vez y queda
cacheada en el esquema `recon`; --rebuild fuerza rehacerla.

Requisitos: la base debe tener las tres entregas, o sea 2024_2C y 2025_1C en
canonical.cursada_historica_history / porcentaje_avance_history, y 2025_2C en las
tablas canonical vigentes. Verificación rápida:
    select academic_period, count(*) from canonical.cursada_historica_history group by 1;
"""

import argparse
import importlib.util
import json
import os
import pathlib
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter
from scipy.stats import kendalltau, spearmanr
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter5"
)

# Las tres entregas, en orden cronológico. La última vive en las tablas canonical
# vigentes; las anteriores, en las tablas de historial que escribe el pre_hook.
ENTREGAS = ["2024_2C", "2025_1C", "2025_2C"]
ENTREGA_VIGENTE = "2025_2C"

# Modelo de referencia de la tesis: GradientBoosting con corte embargado 2021_1C.
TRAIN_CUTOFF = "2021_1C"
GBM_KWARGS = dict(n_estimators=100, max_depth=3, subsample=0.8, random_state=42)

# Paleta validada para daltonismo (Okabe-Ito) y par de estados del panel (b).
C_ENTREGA = {"2024_2C": "#0072B2", "2025_1C": "#E69F00", "2025_2C": "#009E73"}
C_VACIA, C_CERRADO = "#d95f02", "#08519c"
INK, MUTED, GRID = "#1a1a1a", "#5c5c5c", "#dcdcdc"


# ── drift_utils: se carga por ruta porque predun_dagster/__init__.py importa
#    dagster, que no existe en el entorno eda-predun. El módulo en sí es puro
#    (numpy + pandas). Importarlo en lugar de reimplementar el PSI es lo que
#    garantiza que este análisis mida lo mismo que el activo de Dagster.
def _load_drift_utils():
    path = REPO_ROOT / "predun_dagster" / "predun_dagster" / "drift_utils.py"
    spec = importlib.util.spec_from_file_location("drift_utils", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DU = _load_drift_utils()
FEATURES_NUM = DU.FEATURES_NUM          # 9 numéricas, sin materias_cum
FEATURES_CAT = DU.FEATURES_CAT          # cod_carrera
NUM_COLS = FEATURES_NUM + ["materias_cum"]
MODEL_COLS = NUM_COLS + FEATURES_CAT    # 11 features del modelo


def shift_period(period: str, k: int) -> str:
    """Período k cuatrimestres antes (k>0) o después (k<0)."""
    return DU.shift_period(period, k)


def recon_table(entrega: str) -> str:
    return f"recon.panel_{entrega.lower()}"


# ── 1. Reconstrucción del panel por entrega ───────────────────────────────────

PANEL_SQL = r"""
DROP TABLE IF EXISTS {target};
CREATE TABLE {target} AS
with ch as (
    select * from {ch_src} where academic_period = '{entrega}'
),
pa as (
    select * from {pa_src} where academic_period = '{entrega}'
),
cursadas_dated as (
    select legajo, cod_carrera,
        case
            when extract(month from trim(fecha)::date) between 3 and 8
                then make_date(extract(year from trim(fecha)::date)::int, 3, 1)
            when extract(month from trim(fecha)::date) >= 9
                then make_date(extract(year from trim(fecha)::date)::int, 9, 1)
            else make_date(extract(year from trim(fecha)::date)::int - 1, 9, 1)
        end as period_start
    from ch
    where trim(coalesce(fecha, '')) ~ '^\d{{4}}-\d{{2}}-\d{{2}}$'
),
data_bounds as (select max(period_start) as last_period_start from cursadas_dated),
periods as (
    select gs.period_start::date as period_start,
        concat(date_part('year', gs.period_start)::int, '_',
               case when date_part('month', gs.period_start) = 3 then '1C' else '2C' end) as academic_period
    from generate_series('2011-03-01'::date, '2035-09-01'::date, interval '6 months') as gs(period_start)
    cross join data_bounds db
    where gs.period_start <= db.last_period_start
),
actividad_legajo as (select distinct legajo, period_start from cursadas_dated),
finalizacion_estimada as (
    select distinct legajo from pa
    where coalesce(case when trim(porcentaje_avance) ~ '^\d+([.,]\d+)?$'
                        then replace(trim(porcentaje_avance), ',', '.')::numeric end, 0) >= 90
),
legajo_grid as (
    select lf.legajo, p.academic_period, p.period_start,
           case when al.legajo is not null then 1 else 0 end as activo_en_periodo
    from (select legajo, min(period_start) as first_ps from cursadas_dated group by 1) lf
    join periods p on p.period_start >= lf.first_ps
    left join actividad_legajo al on al.legajo = lf.legajo and al.period_start = p.period_start
),
legajo_estado as (
    select legajo, academic_period, period_start, activo_en_periodo,
        case when max(activo_en_periodo) over w_past = 1 then 1 else 0 end as at_risk,
        max(activo_en_periodo) over w_fut as max_act_futura,
        count(*) over w_fut as n_periodos_futuros_obs
    from legajo_grid
    window w_past as (partition by legajo order by period_start rows between 3 preceding and current row),
           w_fut  as (partition by legajo order by period_start rows between 1 following and 4 following)
),
labels as (
    select le.legajo, le.academic_period, le.period_start, le.activo_en_periodo, le.at_risk,
        case when le.n_periodos_futuros_obs < 4 then null
             when coalesce(le.max_act_futura, 0) = 1 then 0
             when g.legajo is not null then 0
             else 1 end as dropout_next
    from legajo_estado le
    left join finalizacion_estimada g on g.legajo = le.legajo
),
carrera_dominante as (
    select distinct on (legajo, period_start) legajo, period_start, cod_carrera
    from (select legajo, cod_carrera, period_start, count(*) as n from cursadas_dated group by 1,2,3) z
    order by legajo, period_start, n desc, cod_carrera
),
carrera_ff as (
    select legajo, period_start, max(cod_carrera) over (partition by legajo, grp) as cod_carrera
    from (
        select lg.legajo, lg.period_start, cd.cod_carrera,
            count(cd.cod_carrera) over (partition by lg.legajo order by lg.period_start
                 rows between unbounded preceding and current row) as grp
        from legajo_grid lg
        left join carrera_dominante cd on cd.legajo = lg.legajo and cd.period_start = lg.period_start
    ) s
),
cursadas_periodo as (
    select legajo, concat(anio, '_', tipo_cursada) as academic_period,
        count(*) as materias_en_periodo,
        -- macro resultado_aprobado(): Promocionó ∪ Regular
        sum(case when (resultado ilike 'Promoc%' or resultado = 'Regular') then 1 else 0 end) as aprob_en_periodo,
        avg(nullif(nota, '')::numeric) as nota_media_en_periodo
    from ch group by 1,2
),
panel_raw as (
    select lb.legajo, cf.cod_carrera, lb.academic_period, lb.at_risk, lb.dropout_next,
        coalesce(cp.materias_en_periodo, 0) as materias_en_periodo,
        coalesce(cp.aprob_en_periodo, 0) as aprob_en_periodo,
        cp.nota_media_en_periodo,
        sum(coalesce(cp.materias_en_periodo, 0)) over w as materias_win3,
        sum(coalesce(cp.aprob_en_periodo, 0)) over w as aprob_win3,
        avg(cp.nota_media_en_periodo) over w as nota_win3,
        sum(coalesce(cp.materias_en_periodo, 0)) over wcum as materias_cum,
        case when coalesce(cp.materias_en_periodo, 0) > 0
             then cp.aprob_en_periodo::numeric / cp.materias_en_periodo else null end as aprob_rate_period,
        case when sum(coalesce(cp.materias_en_periodo, 0)) over w > 0
             then sum(coalesce(cp.aprob_en_periodo, 0)) over w::numeric
                  / sum(coalesce(cp.materias_en_periodo, 0)) over w
             else null end as aprob_rate_win3,
        (lb.period_start - max(case when lb.activo_en_periodo = 1 then lb.period_start end)
             over wcum)::int as dias_desde_ult_actividad
    from labels lb
    left join carrera_ff cf on cf.legajo = lb.legajo and cf.period_start = lb.period_start
    left join cursadas_periodo cp on cp.legajo = lb.legajo and cp.academic_period = lb.academic_period
    window w as (partition by lb.legajo order by lb.period_start rows between 3 preceding and current row),
           wcum as (partition by lb.legajo order by lb.period_start rows between unbounded preceding and current row)
)
select legajo, cod_carrera, academic_period, materias_en_periodo, aprob_en_periodo,
       nota_media_en_periodo, materias_win3, aprob_win3, nota_win3, materias_cum,
       aprob_rate_period, aprob_rate_win3, dias_desde_ult_actividad, at_risk, dropout_next
from panel_raw;
"""


def build_panels(engine, rebuild: bool = False):
    """Materializa recon.panel_<entrega> para las tres entregas."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS recon"))
        conn.commit()

    for entrega in ENTREGAS:
        target = recon_table(entrega)
        if not rebuild:
            exists = pd.read_sql(
                "SELECT to_regclass(%(t)s) IS NOT NULL AS ok", engine, params={"t": target}
            ).iloc[0, 0]
            if exists:
                n = pd.read_sql(f"SELECT count(*) n FROM {target}", engine).iloc[0, 0]
                print(f"  {entrega}: ya existe ({n:,} filas). Usar --rebuild para rehacerlo.")
                continue

        vigente = entrega == ENTREGA_VIGENTE
        sql = PANEL_SQL.format(
            target=target,
            entrega=entrega,
            ch_src="canonical.cursada_historica" if vigente else "canonical.cursada_historica_history",
            pa_src="canonical.porcentaje_avance" if vigente else "canonical.porcentaje_avance_history",
        )
        print(f"  {entrega}: reconstruyendo panel...", flush=True)
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        n = pd.read_sql(f"SELECT count(*) n FROM {target}", engine).iloc[0, 0]
        print(f"  {entrega}: {n:,} filas")


def load_panel(engine, entrega: str, at_risk_only: bool = True) -> pd.DataFrame:
    # El ORDER BY no es cosmético. GradientBoostingClassifier usa subsample=0.8 y
    # elige las filas de cada etapa por POSICIÓN, así que el orden en que Postgres
    # devuelve las filas cambia el modelo entrenado y mueve los resultados en la
    # tercera decimal. (legajo, academic_period) es clave del panel, o sea un orden
    # total, y con eso la corrida es reproducible entre máquinas y entre ejecuciones.
    where = "WHERE at_risk = 1" if at_risk_only else ""
    df = pd.read_sql(
        f"SELECT * FROM {recon_table(entrega)} {where} ORDER BY legajo, academic_period",
        engine,
    )
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    return df


# ── 2. PSI entre entregas, en dos ventanas ────────────────────────────────────

def psi_entre_entregas(paneles: dict) -> list:
    """Tabla 5.12. Para cada par sucesivo, PSI en la ventana de reconciliación
    (períodos que ambas entregas dan por cerrados) y en la ventana completa."""
    filas = []
    for anterior, nueva in zip(ENTREGAS, ENTREGAS[1:]):
        ref, cur = paneles[anterior], paneles[nueva]
        frontera = ref.academic_period.max()
        ventana = shift_period(frontera, 1)   # último período cerrado en AMBAS

        for etiqueta, r, c in [
            ("completa", ref, cur),
            ("reconciliacion", ref[ref.academic_period <= ventana],
                               cur[cur.academic_period <= ventana]),
        ]:
            d = DU.compute_feature_drift(
                r, c, features_num=FEATURES_NUM, features_cat=FEATURES_CAT, include_label=True
            )
            top = d.iloc[0]
            filas.append({
                "par": f"{anterior} -> {nueva}",
                "ventana": etiqueta,
                "hasta": ventana if etiqueta == "reconciliacion" else cur.academic_period.max(),
                "n_ref": len(r), "n_cur": len(c),
                "psi_max": float(top.psi_value),
                "variable": top.feature_name if top.psi_value > 0 else None,
                "detalle": d[["feature_name", "psi_value", "drift_level"]].to_dict("records"),
            })
    return filas


# ── 3. Conciliación por conteo de eventos ─────────────────────────────────────

EVENTOS_SQL = """
CREATE TEMP TABLE ev AS
  SELECT academic_period AS entrega, evento_hash,
         md5(concat_ws('|', legajo, cod_carrera, cod_materia, anio,
                       tipo_cursada, nota, fecha, resultado)) AS hecho_hash
  FROM canonical.cursada_historica_history
  UNION ALL
  SELECT academic_period, evento_hash,
         md5(concat_ws('|', legajo, cod_carrera, cod_materia, anio,
                       tipo_cursada, nota, fecha, resultado))
  FROM canonical.cursada_historica;
"""

CONTEO_SQL = """
WITH p AS (
  SELECT {clave} AS k,
         bool_or(entrega = '2024_2C') d1,
         bool_or(entrega = '2025_1C') d2,
         bool_or(entrega = '2025_2C') d3
  FROM ev GROUP BY 1)
SELECT count(*) FILTER (WHERE d1 AND NOT d2) AS baja_1a2,
       count(*) FILTER (WHERE NOT d1 AND d2) AS alta_1a2,
       count(*) FILTER (WHERE d2 AND NOT d3) AS baja_2a3,
       count(*) FILTER (WHERE NOT d2 AND d3) AS alta_2a3
FROM p;
"""


def conciliacion_eventos(engine) -> dict:
    """Altas y bajas de eventos entre entregas, con la clave del sistema
    (evento_hash, incluye nom_materia/nom_carrera) y con una clave estable
    restringida a los campos que identifican el evento."""
    out = {}
    with engine.connect() as conn:
        conn.execute(text(EVENTOS_SQL))
        for nombre, clave in [("evento_hash", "evento_hash"), ("clave_estable", "hecho_hash")]:
            out[nombre] = pd.read_sql(CONTEO_SQL.format(clave=clave), conn).iloc[0].to_dict()
        totales = pd.read_sql(
            "SELECT entrega, count(*) n FROM ev GROUP BY 1 ORDER BY 1", conn
        ).set_index("entrega")["n"].to_dict()
    out["eventos_por_entrega"] = {k: int(v) for k, v in totales.items()}
    return out


# ── 4. Perfil de la frontera ──────────────────────────────────────────────────

COBERTURA_SQL = r"""
WITH ev AS (
  SELECT academic_period e, trim(fecha)::date f FROM canonical.cursada_historica_history
   WHERE trim(coalesce(fecha,'')) ~ '^\d{4}-\d{2}-\d{2}$'
  UNION ALL
  SELECT academic_period, trim(fecha)::date FROM canonical.cursada_historica
   WHERE trim(coalesce(fecha,'')) ~ '^\d{4}-\d{2}-\d{2}$')
SELECT e AS entrega,
  (CASE WHEN extract(month FROM f) BETWEEN 3 AND 8 THEN extract(year FROM f)::int
        WHEN extract(month FROM f) >= 9 THEN extract(year FROM f)::int
        ELSE extract(year FROM f)::int - 1 END)::text || '_' ||
  (CASE WHEN extract(month FROM f) BETWEEN 3 AND 8 THEN '1C' ELSE '2C' END) AS periodo,
  count(*) n, max(f) ultima_acta
FROM ev GROUP BY 1, 2 ORDER BY 2, 1;
"""


def perfil_frontera(engine, paneles: dict):
    """Tabla 5.13 y datos del panel (a) de la figura."""
    d = pd.read_sql(COBERTURA_SQL, engine)
    piv = d.pivot(index="periodo", columns="entrega", values="n").fillna(0).sort_index()

    filas = []
    for entrega in ENTREGAS:
        panel = paneles[entrega]
        ultimo = panel.academic_period.max()
        borde = panel[panel.academic_period == ultimo]
        eventos = int(piv.loc[ultimo, entrega]) if ultimo in piv.index else 0
        final = int(piv.loc[ultimo, ENTREGA_VIGENTE]) if ultimo in piv.index else 0
        ultima_acta = d[(d.entrega == entrega)].ultima_acta.max()
        filas.append({
            "entrega": entrega,
            "ultimo_periodo": ultimo,
            "ultima_acta": str(ultima_acta),
            "eventos": eventos,
            "eventos_al_cierre": final,
            "cobertura_pct": round(100 * eventos / final, 2) if final else None,
            "filas_en_riesgo": len(borde),
            "sin_actividad_pct": round(100 * (borde.materias_en_periodo == 0).mean(), 1),
        })
    return filas, piv


# ── 5. Efecto de la frontera vacía sobre el scoring ───────────────────────────

def entrenar_referencia(paneles: dict) -> Pipeline:
    """Modelo de referencia de la tesis, sobre el panel de la entrega vigente."""
    train = paneles[ENTREGA_VIGENTE]
    train = train[(train.dropout_next.notna()) & (train.academic_period <= TRAIN_CUTOFF)]
    pipe = Pipeline([
        ("prep", ColumnTransformer([
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                              ("sc", StandardScaler())]), NUM_COLS),
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                              ("oh", OneHotEncoder(handle_unknown="ignore"))]), FEATURES_CAT),
        ])),
        ("clf", GradientBoostingClassifier(**GBM_KWARGS)),
    ])
    pipe.fit(train[MODEL_COLS], train.dropout_next.astype(int))
    return pipe, len(train)


def efecto_frontera(pipe, paneles: dict) -> list:
    """Tabla 5.14. Scorea los mismos legajos y el mismo período con el mismo
    modelo, cambiando solo la entrega desde la que se lee la fila."""
    filas = []
    for anterior, nueva in zip(ENTREGAS, ENTREGAS[1:]):
        periodo = paneles[anterior].academic_period.max()
        a = paneles[anterior]
        b = paneles[nueva]
        a = a[a.academic_period == periodo]
        b = b[b.academic_period == periodo]
        comunes = sorted(set(a.legajo) & set(b.legajo))
        if not comunes:
            continue
        a = a[a.legajo.isin(comunes)].sort_values("legajo").reset_index(drop=True)
        b = b[b.legajo.isin(comunes)].sort_values("legajo").reset_index(drop=True)

        pa = pipe.predict_proba(a[MODEL_COLS])[:, 1]
        pb = pipe.predict_proba(b[MODEL_COLS])[:, 1]

        k = max(1, int(0.10 * len(pa)))
        top_a, top_b = set(np.argsort(-pa)[:k]), set(np.argsort(-pb)[:k])
        filas.append({
            "ciclo": anterior,
            "periodo": periodo,
            "n_legajos": len(comunes),
            "prob_media_frontera_vacia": round(float(pa.mean()), 3),
            "prob_media_periodo_cerrado": round(float(pb.mean()), 3),
            "diferencia": round(float(pa.mean() - pb.mean()), 3),
            "sobre_05_frontera_vacia_pct": round(float((pa > 0.5).mean() * 100), 1),
            "sobre_05_periodo_cerrado_pct": round(float((pb > 0.5).mean() * 100), 1),
            "spearman": round(float(spearmanr(pa, pb).statistic), 3),
            "kendall": round(float(kendalltau(pa, pb).statistic), 3),
            "jaccard_top10": round(len(top_a & top_b) / len(top_a | top_b), 3),
            "top10_coinciden": f"{len(top_a & top_b)} de {k}",
            "_probs": (pa, pb),   # para la figura; se descarta al serializar
        })
    return filas


# ── 6. Figuras ────────────────────────────────────────────────────────────────
#
# Dos figuras separadas, una por archivo. Antes eran dos paneles de una sola
# imagen y, al reducirla al ancho de la página, había que hacer zoom para leer
# las etiquetas de período y los valores de la frontera.

def _estilo_base():
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
        "xtick.color": MUTED, "ytick.color": MUTED, "figure.facecolor": "white",
    })


def plot_cobertura(piv: pd.DataFrame, frontera: list, out_dir: str) -> str:
    """Cobertura de cada período del calendario en cada entrega.

    En la historia cerrada las tres series coinciden exactamente en el 100 %.
    Se las dibuja con marcadores anidados de mayor a menor tamaño para que las
    tres queden visibles donde se superponen.
    """
    cob = piv.loc[piv.index >= "2017_1C"]
    cob_pct = cob.div(cob[ENTREGA_VIGENTE], axis=0) * 100

    _estilo_base()
    fig, ax = plt.subplots(figsize=(11.0, 4.6))

    x = np.arange(len(cob_pct))
    estilo = [(ENTREGAS[0], "-", "o", 11.0, 3.0),
              (ENTREGAS[1], "--", "s", 7.0, 2.4),
              (ENTREGAS[2], ":", "^", 4.0, 1.9)]
    for entrega, ls, mk, ms, lw in estilo:
        y = cob_pct[entrega].where(cob[entrega] > 0)
        ax.plot(x, y, color=C_ENTREGA[entrega], lw=lw, ls=ls, marker=mk, ms=ms,
                mec="white", mew=0.9, label=f"entrega {entrega}", zorder=3)

    ax.axhline(100, color=GRID, lw=1, zorder=1)
    ax.set_ylim(-10, 124)
    ax.set_xlim(-0.6, len(cob_pct) - 0.3)
    ax.set_xticks(x)
    ax.set_xticklabels(cob_pct.index, rotation=90, fontsize=9.5)
    ax.yaxis.set_major_formatter(PercentFormatter())
    ax.set_ylabel("Cobertura del período\n(% del contenido de la entrega final)", fontsize=11)
    ax.grid(axis="y", color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)

    afectadas = [f for f in frontera
                 if f["cobertura_pct"] is not None and f["cobertura_pct"] < 50]
    for i, f in enumerate(afectadas):
        idx = list(cob_pct.index).index(f["ultimo_periodo"])
        offset = (-7, 16) if i == 0 else (7, 4)
        ax.annotate(f"{f['cobertura_pct']:.2f} %".replace(".", ","),
                    (idx, f["cobertura_pct"]), textcoords="offset points",
                    xytext=offset, ha="right" if i == 0 else "left",
                    fontsize=11, color=C_ENTREGA[f["entrega"]], fontweight="bold")
    if afectadas:
        idx0 = list(cob_pct.index).index(afectadas[0]["ultimo_periodo"])
        ax.annotate("frontera: el período que da nombre\na la entrega llega vacío",
                    xy=(idx0 - 0.3, 16), xytext=(idx0 - 6.9, 52),
                    fontsize=10, color=MUTED, ha="left",
                    arrowprops=dict(arrowstyle="->", color=MUTED, lw=1.0,
                                    connectionstyle="arc3,rad=-0.22"))
    ax.legend(frameon=False, fontsize=10.5, loc="lower left", handlelength=2.6,
              bbox_to_anchor=(0.005, -0.02))

    plt.tight_layout()
    path = os.path.join(out_dir, "reconciliacion_cobertura.png")
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_scoring(efecto: list, out_dir: str) -> str:
    """Distribución de la probabilidad predicha con la frontera vacía y con el
    período ya cerrado, sobre los mismos legajos y con el mismo modelo."""
    caso = efecto[-1]
    pa, pb = caso["_probs"]

    _estilo_base()
    fig, ax = plt.subplots(figsize=(9.6, 4.6))

    # Etiquetas cortas: la leyenda va arriba a la izquierda, sobre el hueco que
    # deja el histograma, y con los nombres de entrega completos se ensancharía
    # hasta pisar las líneas de media. Qué entrega es cada una va en el epígrafe.
    bins = np.linspace(0, 1, 41)
    ax.hist(pa, bins=bins, color=C_VACIA, alpha=0.75, label="frontera vacía", zorder=3)
    ax.hist(pb, bins=bins, color=C_CERRADO, alpha=0.60, label="período cerrado", zorder=2)

    top = ax.get_ylim()[1]
    ax.set_ylim(0, top * 1.14)
    for v, c, ha, dx, fy in [(pb.mean(), C_CERRADO, "right", -7, 0.66),
                             (pa.mean(), C_VACIA, "left", 7, 0.80)]:
        ax.axvline(v, color=c, lw=2.0, ls="--", zorder=4)
        ax.annotate(f"media {v:.3f}".replace(".", ","), (v, top * fy), color=c,
                    fontsize=11.5, fontweight="bold", ha=ha, xytext=(dx, 0),
                    textcoords="offset points")

    ax.set_xlabel("Probabilidad de abandono predicha", fontsize=11)
    ax.set_ylabel(f"Estudiantes (n = {caso['n_legajos']:,})".replace(",", "."), fontsize=11)
    ax.grid(axis="y", color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.legend(frameon=False, fontsize=11, loc="upper left", handlelength=1.5,
              bbox_to_anchor=(0.10, 1.0))

    plt.tight_layout()
    path = os.path.join(out_dir, "reconciliacion_scoring.png")
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return path


# ── 7. Reporte ────────────────────────────────────────────────────────────────

def imprimir(psi, conteo, frontera, efecto, n_train):
    ln = "=" * 78
    print(f"\n{ln}\n  TABLA 5.12 — PSI entre entregas\n{ln}")
    print(f"{'Par':<24}{'Ventana':<16}{'hasta':<10}{'N_ref':>10}{'N_act':>10}{'PSI máx':>10}  Variable")
    for f in sorted(psi, key=lambda r: (r["par"], r["ventana"])):
        var = f["variable"] or "---"
        print(f"{f['par']:<24}{f['ventana']:<16}{f['hasta']:<10}"
              f"{f['n_ref']:>10,}{f['n_cur']:>10,}{f['psi_max']:>10.4f}  {var}")

    print(f"\n{ln}\n  Conciliación por conteo de eventos\n{ln}")
    print(f"{'Clave':<18}{'baja 1->2':>12}{'alta 1->2':>12}{'baja 2->3':>12}{'alta 2->3':>12}")
    for clave in ("evento_hash", "clave_estable"):
        c = conteo[clave]
        print(f"{clave:<18}{c['baja_1a2']:>12,}{c['alta_1a2']:>12,}"
              f"{c['baja_2a3']:>12,}{c['alta_2a3']:>12,}")
    fantasma = conteo["evento_hash"]["baja_1a2"] - conteo["clave_estable"]["baja_1a2"]
    print(f"\n  Bajas fantasma por clave inestable (1->2): {fantasma:,}")

    print(f"\n{ln}\n  TABLA 5.13 — Perfil de la frontera\n{ln}")
    print(f"{'Entrega':<10}{'Últ. período':<14}{'Últ. acta':<13}{'Eventos':>9}"
          f"{'Cobertura':>11}{'En riesgo':>11}{'Sin activ.':>11}")
    for f in frontera:
        cob = f"{f['cobertura_pct']:.2f} %" if f["cobertura_pct"] is not None else "---"
        print(f"{f['entrega']:<10}{f['ultimo_periodo']:<14}{f['ultima_acta']:<13}"
              f"{f['eventos']:>9,}{cob:>11}{f['filas_en_riesgo']:>11,}"
              f"{f['sin_actividad_pct']:>10.1f} %")

    print(f"\n{ln}\n  TABLA 5.14 — Efecto de la frontera vacía sobre el scoring\n{ln}")
    print(f"  Modelo de referencia: GradientBoosting, corte {TRAIN_CUTOFF}, "
          f"{n_train:,} filas de entrenamiento\n")
    campos = [
        ("Legajos comparados", "n_legajos", "{:,}"),
        ("Prob. media, frontera vacía", "prob_media_frontera_vacia", "{:.3f}"),
        ("Prob. media, período cerrado", "prob_media_periodo_cerrado", "{:.3f}"),
        ("Diferencia", "diferencia", "{:+.3f}"),
        ("% sobre 0,5 con frontera vacía", "sobre_05_frontera_vacia_pct", "{:.1f}"),
        ("% sobre 0,5 con período cerrado", "sobre_05_periodo_cerrado_pct", "{:.1f}"),
        ("Spearman", "spearman", "{:.3f}"),
        ("Solapamiento top-10 % (Jaccard)", "jaccard_top10", "{:.3f}"),
        ("Top-10 % que coinciden", "top10_coinciden", "{}"),
    ]
    print(f"{'':36s}" + "".join(f"{'ciclo ' + e['ciclo']:>20s}" for e in efecto))
    for etiqueta, k, fmt in campos:
        print(f"{etiqueta:<36s}" + "".join(f"{fmt.format(e[k]):>20s}" for e in efecto))


def main(rebuild: bool, con_figura: bool):
    engine = create_engine(PG_URI)

    print("\n  Reconstruyendo paneles por entrega desde canonical.*_history...")
    build_panels(engine, rebuild=rebuild)

    paneles = {e: load_panel(engine, e) for e in ENTREGAS}
    for e, p in paneles.items():
        print(f"  {e}: {len(p):,} filas en riesgo, último período {p.academic_period.max()}")

    print("\n  Calculando PSI entre entregas...")
    psi = psi_entre_entregas(paneles)

    print("  Conciliando eventos...")
    conteo = conciliacion_eventos(engine)

    print("  Midiendo el perfil de la frontera...")
    frontera, piv = perfil_frontera(engine, paneles)

    print(f"  Entrenando el modelo de referencia (corte {TRAIN_CUTOFF})...")
    pipe, n_train = entrenar_referencia(paneles)
    efecto = efecto_frontera(pipe, paneles)

    imprimir(psi, conteo, frontera, efecto, n_train)

    if con_figura:
        print()
        for path in (plot_cobertura(piv, frontera, THESIS_FIGS_DIR),
                     plot_scoring(efecto, THESIS_FIGS_DIR)):
            print(f"  Figura guardada: {path}")

    salida = {
        "psi_entre_entregas": psi,
        "conciliacion_eventos": conteo,
        "perfil_frontera": frontera,
        "efecto_frontera": [{k: v for k, v in e.items() if k != "_probs"} for e in efecto],
        "modelo_referencia": {"corte": TRAIN_CUTOFF, "n_train": n_train, **GBM_KWARGS},
    }
    json_path = os.path.join(THESIS_FIGS_DIR, "reconciliacion_entregas.json")
    with open(json_path, "w") as f:
        json.dump(salida, f, indent=2, default=str, ensure_ascii=False)
    print(f"  Métricas guardadas: {json_path}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true",
                        help="Rehace los paneles recon.* aunque ya existan")
    parser.add_argument("--no-figure", action="store_true",
                        help="Solo imprime las tablas, sin generar las figuras")
    args = parser.parse_args()
    main(rebuild=args.rebuild, con_figura=not args.no_figure)
