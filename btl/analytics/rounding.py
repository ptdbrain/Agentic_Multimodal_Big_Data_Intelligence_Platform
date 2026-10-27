"""Metric rounding shared by local analytics; matches Spark SQL round (half up)."""
from decimal import Decimal, ROUND_HALF_UP, localcontext

import pandas as pd


def round_metric(value, digits: int = 2):
    if value is None or pd.isna(value):
        return float('nan')
    number = Decimal(str(value))
    with localcontext() as context:
        context.prec = max(28, len(number.as_tuple().digits) + abs(number.adjusted()) + digits + 4)
        return float(number.quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))


def round_series(values: pd.Series, digits: int = 2) -> pd.Series:
    return values.map(lambda value: round_metric(value, digits))
