"""
Control de datos personales en la ingesta (Cap. 4, Tratamiento de los datos personales).

Verifica sobre las tres entregas institucionales:

1. Ningun descriptor YAML de ingesta deja pasar al target una columna identificatoria.
2. Ningun CSV limpio de la raiz del periodo tiene una columna identificatoria. Son los
   unicos archivos que lee 04_ingest_to_staging.py, que hace un glob no recursivo.
3. Antes y despues: cada columna identificatoria que traen los archivos crudos de
   <periodo>/raw/ desaparece en los archivos limpios de ese periodo.

Solo se leen nombres de columna, nunca valores.

Uso:
    conda run -n eda-predun python scripts/check_pii.py
"""
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAPPINGS = ROOT / "predun_dagster" / "ingestion" / "mappings" / "fix_and_clean"
DATA = ROOT / "data-private"
ENTREGA = re.compile(r"^\d{4}_\dC$")

# Columnas que identifican directamente a la persona y no deben llegar a la base.
PII = {
    "persona",
    "alumno",
    "tipo_documento",
    "nro_documento",
    "apellido",
    "nombres",
    "email",
    "telefono_numero",
    "periodo_lectivo_calle",
    "periodo_lectivo_numero",
    "periodo_lectivo_piso",
    "periodo_lectivo_departamento",
}


def col_names(entries):
    """Las listas de columnas admiten strings o dicts de un solo par."""
    out = []
    for c in entries or []:
        out.append(next(iter(c)) if isinstance(c, dict) else c)
    return set(out)


def header(path):
    linea = path.open(encoding="utf-8", errors="replace").readline()
    return {c.strip().strip('"').lower() for c in linea.split(",")}


def entregas():
    return sorted(p for p in DATA.iterdir() if p.is_dir() and ENTREGA.match(p.name))


def check_mappings():
    fallas = []
    archivos = sorted(MAPPINGS.glob("*.yaml"))
    if not archivos:
        fallas.append(f"no se encontro ningun descriptor en {MAPPINGS}")
    for path in archivos:
        cols = yaml.safe_load(path.read_text(encoding="utf-8"))["columns"]
        source = col_names(cols.get("source"))
        target = col_names(cols.get("target")) or (source - col_names(cols.get("drop")))
        leak = sorted(PII & target)
        print(f"  [{'OK' if not leak else 'FALLA'}] {path.name:28s} "
              f"descarta {len(PII & source):2d} de {len(source):2d} columnas")
        if leak:
            fallas.append(f"{path.name} deja pasar {leak}")
    return fallas


def check_limpios():
    fallas = []
    for periodo in entregas():
        for csv_path in sorted(periodo.glob("*.csv")):
            cols = header(csv_path)
            leak = sorted(PII & cols)
            print(f"  [{'OK' if not leak else 'FALLA'}] "
                  f"{periodo.name}/{csv_path.name:38s} {len(cols):2d} columnas")
            if leak:
                fallas.append(f"{periodo.name}/{csv_path.name} tiene {leak}")
    return fallas


def check_antes_despues():
    fallas = []
    for periodo in entregas():
        raw = periodo / "raw"
        if not raw.is_dir():
            continue
        limpias = set()
        for csv_path in periodo.glob("*.csv"):
            limpias |= header(csv_path)
        for cruda in sorted(raw.glob("*.csv")):
            traia = PII & header(cruda)
            sobreviven = sorted(traia & limpias)
            print(f"  [{'OK' if not sobreviven else 'FALLA'}] "
                  f"{periodo.name}/raw/{cruda.name:34s} trae {len(traia):2d}, "
                  f"sobreviven {len(sobreviven)}")
            if sobreviven:
                fallas.append(f"{periodo.name}/raw/{cruda.name}: sobreviven {sobreviven}")
    return fallas


def main():
    fallas = []
    print("1. Descriptores YAML de ingesta")
    fallas += check_mappings()
    print("\n2. CSV limpios que lee el cargador de staging")
    fallas += check_limpios()
    print("\n3. Antes y despues de la ingesta")
    fallas += check_antes_despues()

    print()
    if fallas:
        print(f"FALLA: {len(fallas)} problemas")
        for f in fallas:
            print(f"  - {f}")
        return 1
    print("OK: ninguna columna identificatoria llega a la base")
    return 0


if __name__ == "__main__":
    sys.exit(main())
