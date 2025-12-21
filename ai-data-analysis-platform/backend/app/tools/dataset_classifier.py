def classify_dataset(columns: list[str]) -> str:
    cols = " ".join(c.lower() for c in columns)

    if any(x in cols for x in ["sales", "revenue", "amount", "price"]):
        return "Sales Data"

    if any(x in cols for x in ["customer", "user", "gender", "age"]):
        return "Customer Data"

    if any(x in cols for x in ["transaction", "order", "invoice"]):
        return "Transaction Data"

    return "Generic Tabular Data"
