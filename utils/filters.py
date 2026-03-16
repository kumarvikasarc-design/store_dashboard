import pandas as pd

FILTER_ORDER = ["Brand", "Region", "State", "City", "Type", "Outlet"]


def apply_multi_filters(df: pd.DataFrame, selections: dict) -> pd.DataFrame:
    """
    Apply Dash multi-select filters safely.
    selections = {"Brand": [...], "Region": [...], ...}
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    for col in FILTER_ORDER:
        values = selections.get(col)

        if not values or col not in out.columns:
            continue

        if not isinstance(values, (list, tuple, set)):
            values = [values]

        out = out[out[col].isin(values)]

    return out


def cascade_options(df: pd.DataFrame, selections: dict):
    """
    Returns dropdown options for all filter columns
    using strict left-to-right cascade logic.
    """
    options = {}

    for i, col in enumerate(FILTER_ORDER):
        prior_filters = {
            k: selections.get(k)
            for k in FILTER_ORDER[:i]
            if selections.get(k)
        }

        filtered = apply_multi_filters(df, prior_filters)

        if col in filtered.columns:
            options[col] = [
                {"label": v, "value": v}
                for v in sorted(filtered[col].dropna().unique())
            ]
        else:
            options[col] = []

    return options
