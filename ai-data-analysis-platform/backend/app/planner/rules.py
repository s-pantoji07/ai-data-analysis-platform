def get_column(metadata: dict, name: str):
    for col in metadata["profiling_summary"]["columns"]:
        if col["name"].lower() == name.lower():
            return col
    return None


def is_numeric(metadata: dict, column: str) -> bool:
    col = get_column(metadata, column)
    return col and col["type"] == "numeric"


def is_categorical(metadata: dict, column: str) -> bool:
    col = get_column(metadata, column)
    return col and col["type"] == "categorical"
