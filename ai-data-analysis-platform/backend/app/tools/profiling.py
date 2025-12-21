import pandas as pd

def profile_dataframe(df: pd.DataFrame)-> dict:

    profile = {
        "row_count":len(df),
        "column_count":len(df.columns),
        "columns":[],
        "missing_values":{},
        "date_columns":[],
    }

    for col in df.columns:
        dtype = str(df[col].dtype)
        missing = int(df[col].isna().sum())

        profile["columns"].append({
            "name":col,
            "dtype":dtype,
        })

        profile["missing_values"][col] = missing

        if "date" in col.lower() or "time" in col.lower():
            profile["date_columns"].append(col)


    return profile
