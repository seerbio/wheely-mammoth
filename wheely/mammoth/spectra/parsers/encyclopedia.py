"""
`wheely.mammoth.spectra.parsers.encyclopedia` -- read spectral information from EncyclopeDIA ELIB
files.
"""

import re as _re
from typing import (
    Union as _Union,
)

import pandas as _pd
from pyspark.sql import (
    Column as _Column,
    SparkSession as _SparkSession,
)

from ...dataset import PsmDataset as _PsmDataset
from .. import SpectraDataset as _SpectraDataset


def read_encyclopedia_elib_pandas(elib_location: str) -> _pd.DataFrame:
    """
    TODO

    Parameters
    ----------
    elib_location

    Returns
    -------

    """
    raise NotImplementedError("TODO")


def read_encyclopedia_elib(elib_location: str) -> _SpectraDataset:
    """
    TODO

    Parameters
    ----------
    elib_location

    Returns
    -------

    """
    raise NotImplementedError("TODO")


def read_encyclopedia_entries(
    psms: _PsmDataset,
    elib_loc_col: _Union[str, _Column] = None,
    file_loc_col: _Union[str, _Column] = None,
    file_loc_patt: _Union[str, _re.Pattern] = None,
    elib_loc_fmt: str = None,
    spark: _SparkSession = None,
) -> _SpectraDataset:
    """
    TODO

    Parameters
    ----------
    psms
    elib_loc_col
    file_loc_col
    file_loc_patt
    elib_loc_fmt
    spark

    Returns
    -------

    """
    raise NotImplementedError("TODO")
