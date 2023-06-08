"""
`scry.output.library.spectra.utils` -- utility methods for handling spectral library peaklists
"""

from typing import (
    Tuple as _Tuple,
    Union as _Union,
)

import pandas as _pd

from pyspark.sql import Column as _Column
from pyspark.sql.functions import (
    PandasUDFType as _PandasUDFType,
    arrays_zip as _arrays_zip,
    col as _col,
    explode as _explode,
    pandas_udf as _pandas_udf,
    transform as _transform,
)

from .dataset import PeaklistType as _PeaklistType


def lists_to_peaklist(
    mz_col: _Union[str, _Column], inten_col: _Union[str, _Column]
) -> _Column:
    """
    Convert two array-typed columns of M/Z and intensity values into an appropriately-structured
    single "peaklist" column.
    """
    return _arrays_zip(mz_col, inten_col)


@_pandas_udf(returnType=_PeaklistType, functionType=_PandasUDFType.GROUPED_AGG)
def rows_to_peaklist(mz_col: _pd.Series, inten_col: _pd.Series) -> _pd.Series:
    """
    This Pandas UDF can be applied to a dataframe of individual peaks, grouped into spectra.
    For each group it will return a single row with all peaks combined into a list of pairs.
    """
    return _pd.Series(list(zip(mz_col, inten_col)))


@_pandas_udf(returnType=_PeaklistType, functionType=_PandasUDFType.GROUPED_AGG)
def pairs_to_peaklist(pair_col: _pd.Series) -> _pd.Series:
    """
    This Pandas UDF can be applied to a dataframe of individual peaks, grouped into spectra.
    For each group it will return a single row with all peaks combined into a list of pairs.
    """
    return _pd.Series(list(pair_col))


def peaklist_to_mzs(peaklist_col: _Union[str, _Column]) -> _Column:
    """
    Convert a single "peaklist" column into a single column containing pairs of M/Z and intensity arrays.
    """
    return _transform(peaklist_col, lambda p: p.getItem(0))


def peaklist_to_intens(peaklist_col: _Union[str, _Column]) -> _Column:
    """
    Convert a single "peaklist" column into a single column containing pairs of M/Z and intensity arrays.
    """
    return _transform(peaklist_col, lambda p: p.getItem(1))


def peaklist_to_lists(
    peaklist_col: _Union[str, _Column]
) -> _Tuple[_Column, _Column]:
    """
    Convert a single "peaklist" column into a single column containing pairs of M/Z and intensity arrays.
    This effectively transposes the peaklist struct.
    """
    return peaklist_to_mzs(peaklist_col), peaklist_to_intens(peaklist_col)


def peaklist_to_pairs(peaklist_col: _Union[str, _Column]) -> _Column:
    """
    Convert a single "peaklist" column into a single array-valued column with a row for each peak in
    each of the input spectra. Each row will contain a two-element array giving (m/z, intensity).

    Note that due to restrictions in Spark SQL it's difficult to split these pairs into separate
    columns. It's typically best to use this pair-based format and `pairs_to_peaklist` than to
    rely on `rows_to_peaklist`.
    """
    return _explode(peaklist_col)
