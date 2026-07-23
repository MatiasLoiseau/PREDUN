#!/usr/bin/env python
"""
Verificación de gates del reproceso de julio 2026 (corrección de duplicación de
actas + vocabulario de resultado).

Se corre después de cada ciclo, entre `dbt run` y la integración al informe.
No modifica nada: solo lee y compara contra los valores esperados.

    conda run -n eda-predun python scripts/verify_gates.py --cycle 2025_2C

Gates:
  G1  canonical  — una fila por evento académico; inflación 0 % en todos los años.
  G2  marts      — cardinalidades del panel y del padrón INVARIANTES respecto del
                   baseline pre-corrección. Son la prueba de que las correcciones
                   tocaron features y no la etiqueta: la duplicación no afectaba
                   `distinct (legajo, period_start)` ni `fecha`. Si G2 falla, se
                   rompió algo distinto de las dos correcciones.
  G3  features   — el quiebre de 2021_1C desapareció: la media de
                   `materias_en_periodo` no salta, y `aprob_rate_period` es
                   estable a ambos lados del corte de entrenamiento.
  G4  deriva     — ninguna feature con PSI alto sin explicación.

Código de salida: 0 si todos los gates pasan, 1 si alguno falla.
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

PG = os.environ.get(
    "PREDUN_PG_URI", "postgresql://siu:siu@localhost:5432/postgres"
)

# Valores esperados por ciclo. Provienen del reproceso de junio 2026
# (anotaciones/reproceso_2026-06.md) y NO deben cambiar con la corrección:
# la etiqueta, el conjunto de riesgo y el padrón son ajenos a la duplicación.
BASELINE = {
    "2024_2C": {"panel": 630_205, "at_risk": 370_188, "modelado": 280_482, "status": 61_989},
    "2025_1C": {"panel": 684_585, "at_risk": 391_425, "modelado": 304_175, "status": 67_878},
    "2025_2C": {"panel": 687_186, "at_risk": 394_362, "modelado": 304_175, "status": 69_779},
}

# Clave de negocio del evento académico (sin metadatos del acta).
BK = ("legajo, cod_carrera, cod_materia, anio, tipo_cursada, "
      "coalesce(nota,''), fecha, resultado")

CORTE_ENTRENAMIENTO = "2021_1C"

results: list[tuple[bool, str, str]] = []


def check(ok: bool, gate: str, msg: str) -> None:
    results.append((ok, gate, msg))
    print(f"  [{'PASS' if ok else 'FAIL'}] {msg}", flush=True)


def guard(fn, gate: str, *args) -> None:
    """Ejecuta un gate; cualquier excepción se reporta como FAIL legible.

    Un error de SQL acá casi siempre significa que el modelo dbt correspondiente
    todavía no se materializó con la versión corregida (p. ej. student_panel aún
    expone promo_* en vez de aprob_*). Es un fallo del gate, no del script.
    """
    try:
        fn(*args)
    except Exception as e:
        detalle = str(e).strip().splitlines()[0][:160]
        check(False, gate, f"{gate} no se pudo evaluar: {type(e).__name__}: {detalle}")


def g1_canonical(eng) -> None:
    print("\nG1 — canonical.cursada_historica")
    n = pd.read_sql("select count(*) n from canonical.cursada_historica", eng).n[0]
    ev = pd.read_sql(
        f"select count(*) n from (select 1 from canonical.cursada_historica group by {BK}) z",
        eng,
    ).n[0]
    check(n == ev, "G1", f"una fila por evento: {n:,} filas / {ev:,} eventos")

    infl = pd.read_sql(
        f"""select anio, count(*) filas,
                   count(distinct ({BK})) eventos
            from canonical.cursada_historica group by 1 order by 1""",
        eng,
    )
    infl["pct"] = (100 * infl.filas / infl.eventos - 100).round(2)
    peor = infl.loc[infl.pct.abs().idxmax()]
    check(
        bool((infl.pct.abs() < 0.01).all()),
        "G1",
        f"inflación 0 % en los {len(infl)} años (peor: {peor.anio} = {peor.pct} %)",
    )

    cols = pd.read_sql(
        """select column_name from information_schema.columns
           where table_schema='canonical' and table_name='cursada_historica'""",
        eng,
    ).column_name.tolist()
    check(
        {"evento_hash", "n_actas", "origenes"}.issubset(cols),
        "G1",
        "columnas de auditoría presentes (evento_hash, n_actas, origenes)",
    )


def g2_marts(eng, cycle: str) -> None:
    print(f"\nG2 — cardinalidades invariantes ({cycle})")
    exp = BASELINE.get(cycle)
    if exp is None:
        check(False, "G2", f"ciclo '{cycle}' sin baseline registrado")
        return

    got = pd.read_sql(
        """select
             (select count(*) from marts.student_panel)                                  as panel,
             (select count(*) from marts.student_panel where at_risk=1)                  as at_risk,
             (select count(*) from marts.student_panel
                where at_risk=1 and dropout_next is not null)                            as modelado,
             (select count(*) from marts.student_status)                                 as status""",
        eng,
    ).iloc[0]

    for k, v in exp.items():
        check(
            int(got[k]) == v,
            "G2",
            f"{k}: {int(got[k]):,} (esperado {v:,})",
        )


def g3_features(eng) -> None:
    print("\nG3 — el quiebre de 2021_1C desapareció")
    d = pd.read_sql(
        """select academic_period,
                  avg(materias_en_periodo) media_materias,
                  count(*) n
           from marts.student_panel
           where materias_en_periodo > 0
           group by 1 order by 1""",
        eng,
    )
    d = d[d.n >= 500].reset_index(drop=True)
    d["salto"] = (d.media_materias / d.media_materias.shift(1) - 1).abs()
    peor = d.loc[d.salto.idxmax()] if d.salto.notna().any() else None
    check(
        bool((d.salto.fillna(0) < 0.20).all()),
        "G3",
        "sin saltos > 20 % en materias_en_periodo"
        + (f" (peor: {peor.academic_period} = {peor.salto:.1%})" if peor is not None else ""),
    )

    r = pd.read_sql(
        f"""select case when academic_period <= '{CORTE_ENTRENAMIENTO}'
                        then 'ref' else 'act' end regimen,
                   avg(aprob_rate_period) media
            from marts.student_panel
            where at_risk = 1 and aprob_rate_period is not null
            group by 1""",
        eng,
    ).set_index("regimen").media
    delta = abs(float(r["ref"]) - float(r["act"]))
    check(
        delta < 0.10,
        "G3",
        f"aprob_rate_period estable: ref {r['ref']:.3f} vs act {r['act']:.3f} (Δ {delta:.3f})",
    )


def g4_drift(eng, cycle: str) -> None:
    print(f"\nG4 — deriva ({cycle})")
    try:
        d = pd.read_sql(
            "select feature_name, psi_value, drift_level from predictions.drift_metrics "
            "where cycle_period = %(c)s order by psi_value desc",
            eng, params={"c": cycle},
        )
    except Exception as e:  # tabla aún inexistente en el primer ciclo
        check(False, "G4", f"no se pudo leer predictions.drift_metrics ({e.__class__.__name__})")
        return

    if d.empty:
        check(False, "G4", f"sin filas de deriva para el ciclo {cycle} (¿corrió drift_train_predict?)")
        return

    altas = d[d.psi_value >= 0.25]
    print(d.to_string(index=False))
    # cod_carrera con deriva alta es esperable y está explicado (composición de
    # matrícula: las carreras nuevas pesan más en las cohortes recientes).
    inesperadas = altas[altas.feature_name != "cod_carrera"]
    check(
        inesperadas.empty,
        "G4",
        "sin deriva alta inesperada"
        + (f" — revisar: {', '.join(inesperadas.feature_name)}" if not inesperadas.empty else ""),
    )

    aprob = d[d.feature_name.str.startswith("aprob_")]
    if not aprob.empty:
        check(
            bool((aprob.psi_value < 0.25).all()),
            "G4",
            f"features aprob_* por debajo de deriva alta (máx {aprob.psi_value.max():.3f})",
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cycle", required=True, choices=sorted(BASELINE))
    ap.add_argument("--skip-drift", action="store_true",
                    help="omitir G4 (útil antes de correr drift_train_predict)")
    args = ap.parse_args()

    eng = create_engine(PG)
    print(f"Verificando gates del ciclo {args.cycle}")

    guard(g1_canonical, "G1", eng)
    guard(g2_marts, "G2", eng, args.cycle)
    guard(g3_features, "G3", eng)
    if not args.skip_drift:
        guard(g4_drift, "G4", eng, args.cycle)

    fallos = [r for r in results if not r[0]]
    print(f"\n{'=' * 60}")
    if fallos:
        print(f"{len(fallos)} de {len(results)} gates FALLARON:")
        for _, gate, msg in fallos:
            print(f"  {gate}: {msg}")
        print("\nNo integrar resultados al informe hasta resolverlos.")
        return 1
    print(f"Los {len(results)} gates pasaron. Ciclo {args.cycle} listo para integrar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
