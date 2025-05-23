fill_nan_with_default_values = r"""
def fill_nan_with_defaults(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {}
    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_numeric_dtype(dtype):
            defaults[col] = 0
        elif pd.api.types.is_bool_dtype(dtype):
            defaults[col] = False
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            defaults[col] = pd.Timestamp(0)
        elif pd.api.types.is_categorical_dtype(dtype):
            defaults[col] = df[col].cat.categories[0] if len(df[col].cat.categories) > 0 else ''
        else:
            # Default for object, string, or unknown types
            defaults[col] = ''
    
    return df.fillna(value=defaults)
"""