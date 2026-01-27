from pathlib import Path
from pandas import pd


def find_widest_col_name(source_dir: Path, filename_pattern: str) -> list:
    column_stack = []
    for file in source_dir.rglob(filename_pattern):
        try:
            df = pd.read_csv(file, encoding="utf-8-sig", engine="python")
            if not column_stack:
                column_stack.extend(list(df.columns))
            else:
                for col in df.columns:
                    if col not in column_stack:
                        column_stack.append(col)
        except Exception as e:
            print(f"Error: {e}")
    print("col_name欄位計畫完成，定義在list中")
    return column_stack
