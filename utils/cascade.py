def apply_filters(df, filters):
    """
    Apply multiple dimension filters safely.
    filters = { column_name: list_or_none }
    """
    out = df.copy()
    for col, vals in filters.items():
        if vals and col in out.columns:
            out = out[out[col].isin(vals)]
    return out


def build_cascade_options(df, columns):
    """
    Build dropdown options from filtered df.
    """
    opts = {}
    for col in columns:
        if col in df.columns:
            opts[col] = [
                {"label": v, "value": v}
                for v in sorted(df[col].dropna().unique())
            ]
        else:
            opts[col] = []
    return opts
