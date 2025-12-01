#!/usr/bin/env python3
"""
remove_empty_rows.py

Lee un CSV y elimina las filas "vacías" (todas las celdas vacías o espacios).
Guarda el resultado junto al archivo original añadiendo `_fix` antes de la extensión.

Uso:
    python3 utils/remove_empty_rows.py /ruta/al/archivo.csv

Si se desea, pasar --output /ruta/de/salida.csv
El script intenta leer en utf-8 y hace fallback a latin-1 si hay errores de decodificación.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Iterable


def is_empty_row(row: Iterable[str]) -> bool:
    """Devuelve True si todas las celdas de la fila están vacías (después de strip)."""
    # row puede ser una lista vacía (línea en blanco) o lista de strings
    if not row:
        return True
    for cell in row:
        if cell is None:
            continue
        if str(cell).strip() != "":
            return False
    return True


def make_output_path(input_path: str, output_arg: str | None) -> str:
    if output_arg:
        return output_arg
    base, ext = os.path.splitext(input_path)
    return f"{base}_fix{ext}"


def process_csv(input_path: str, output_path: str, encoding: str = "utf-8") -> None:
    """Procesa el CSV en streaming, copiando solo filas no vacías al archivo de salida."""
    with open(input_path, newline="", encoding=encoding) as infile, open(output_path, "w", newline="", encoding=encoding) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        written = 0
        for row in reader:
            if not is_empty_row(row):
                writer.writerow(row)
                written += 1
    print(f"Escrito {written} filas en: {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Eliminar filas vacías de un CSV y guardar con _fix")
    parser.add_argument("input", help="Ruta al archivo CSV de entrada")
    parser.add_argument("--output", "-o", help="Ruta de salida opcional (por defecto añade _fix al nombre)")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.isfile(input_path):
        print(f"Error: archivo no encontrado: {input_path}", file=sys.stderr)
        return 2

    output_path = make_output_path(input_path, args.output)

    # Intentar con utf-8 y fallback a latin-1
    tried_encodings = ["utf-8", "latin-1"]
    for enc in tried_encodings:
        try:
            process_csv(input_path, output_path, encoding=enc)
            return 0
        except UnicodeDecodeError:
            print(f"Advertencia: fallo de decodificación con {enc}, intentando siguiente...", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Error procesando el archivo: {e}", file=sys.stderr)
            return 3

    print("No se pudo leer el archivo con las codificaciones probadas.", file=sys.stderr)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
