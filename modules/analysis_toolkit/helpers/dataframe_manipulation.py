import pandas as pd


def sort_into_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sort a MultiIndex DataFrame into columns based on a specified level order."""
    return df.T.unstack(level=0).swaplevel(0, 1, axis=1).sort_index(axis=1)