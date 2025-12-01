import sys
from pathlib import Path
import pandas as pd

# Defaults: read the historico_cursada.txt for 2025_2C in the workspace
DEFAULT_INPUT = (
    "/Users/matiasloiseau/Workspace/PREDUN/data-private/2025_2C/raw/historico_cursada.txt"
)


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv

    # allow passing input path as first arg, otherwise use default
    input_path = Path(argv[1]) if len(argv) > 1 else Path(DEFAULT_INPUT)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 2

    output_path = input_path.with_suffix('.csv')

    # Read everything as string to preserve leading zeros (e.g., document numbers)
    # and avoid pandas interpreting 'NULL' as NaN — we'll replace 'NULL' explicitly.
    try:
        df = pd.read_csv(
            input_path,
            sep='|',
            engine='python',
            dtype=str,
            encoding='utf-8',
            na_filter=False,  # keep literal strings like 'NULL'
        )
    except Exception as e:
        # Fallback for malformed rows (unequal number of fields). We'll parse manually:
        # - take header from first line
        # - for rows with more fields than header, join the extras into the last column
        # - for rows with fewer fields, pad with empty strings
        from pandas.errors import ParserError

        # If it's not a parser error, re-raise; otherwise attempt manual parse
        if not isinstance(e, ParserError):
            raise

        header = None
        rows = []
        with open(input_path, 'r', encoding='utf-8', errors='replace') as fh:
            for i, raw in enumerate(fh):
                line = raw.rstrip('\n')
                parts = line.split('|')
                if i == 0:
                    header = parts
                    ncol = len(header)
                    continue
                if len(parts) < ncol:
                    parts = parts + [''] * (ncol - len(parts))
                elif len(parts) > ncol:
                    # merge extra fields into last column (keeps data rather than dropping)
                    parts = parts[: ncol - 1] + ['|'.join(parts[ncol - 1 :])]
                rows.append(parts)

        if header is None:
            print(f"Input file appears empty: {input_path}")
            return 3

        df = pd.DataFrame(rows, columns=header)

    # Replace textual NULL markers with empty string
    df = df.replace({'NULL': ''})

    # If there are accidental unnamed trailing columns, drop them
    unnamed_cols = [c for c in df.columns if c.startswith('Unnamed:')]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)

    df.to_csv(output_path, index=False)

    print(f"File saved as: {output_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())