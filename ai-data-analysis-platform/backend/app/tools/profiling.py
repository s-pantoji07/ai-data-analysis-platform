import pandas as pd

def profile_dataframe(df:pd.DataFrame)-> dict:
    profile = {
        "row_count":len(df),
        "column_count":len(df.columns),
        "columns":[],
        "missing_values":{},
        "date_columns":[],
        "numeric_columns":[],
        "categorical_columns":[],
    }

    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        missing = int(series.isna().sum())


        if pd.api.types.is_numeric_dtype(series):
            normalized_type = "numeric"
            profile["numeric_columns"].append(col)
        elif pd.api.types.is_datetime64_any_dtype(series):
            normalized_type = "datetime"
            profile["date_columns"].append(col)
        else:
            normalized_type = "categorical"
            profile["categorical_columns"].append(col)

        if col.lower() in ["year","yr","fy"] and normalized_type =="numeric":
            profile["date_columns"].append(col)

        profile["columns"].append({
            "name":col,
            "dtype":dtype,
            "type":normalized_type
        })

        profile["missing_values"][col] = missing

    return profile