class MetadataValidationError(Exception):
    pass

class ColumnNotFoundError(MetadataValidationError):
    def __init__(self,column : str , available:list[str]):
        super().__init__(
            f"Column '{column}' not found. Available columns: {', '.join(available)}"
        )

class InvalidAggregationError(MetadataValidationError):
    def __init__(self,column:str,dtype:str,function:str):
        super().__init__(
            f"Cannot apply '{function}' aggregation on '{column}' (type: {dtype})"
        )
    