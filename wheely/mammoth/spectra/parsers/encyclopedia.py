"""
`wheely.mammoth.spectra.parsers.encyclopedia` -- read spectral information from EncyclopeDIA ELIB
files.
"""

import re as _re
from typing import (
    Any as _Any,
    Dict as _Dict,
    Union as _Union,
)

import pandas as _pd
from pyspark.sql import (
    functions as _fns,
    types as _typ,
    Column as _Column,
    SparkSession as _SparkSession,
)

from ...dataset import PsmDataset as _PsmDataset
from .. import SpectraDataset as _SpectraDataset


def read_encyclopedia_elib(elib_location: str) -> _SpectraDataset:
    """
    Read a single ELIB and return all its entries.

    Parameters
    ----------
    elib_location The URI of the ELIB

    Returns
    -------
    A `SpectraDataset` with spectral information for all entries in the ELIB. The semantics of the
    set of spectra and peaks depend on hwo the ELIB is created. For converted spectral libraries
    (DLIB extension) or exported chromatogram libraries this will be the set of library entries.
    For single-file ELIBs this will be the set of IDs at the FDR threshold, with unrefined or
    lightly-refined fragment ions. For quantitative ELIBs (created while exporting combined
    quantiative reports) the semantics are unclear.
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
    if elib_loc_col is None:
        if not isinstance(file_loc_patt, _re.Pattern):
            file_loc_patt = _re.compile(file_loc_patt)

        # TODO: vectorize
        id = file_loc_patt.search(file_loc_col).group(0)

        path = elib_loc_fmt.format(id)

        elib_loc_col = ""  # TODO?

    raise NotImplementedError("TODO")


def read_encyclopedia_elib_pandas(elib_location: str) -> _pd.DataFrame:
    """
    Read a single ELIB and return all its entries.

    Parameters
    ----------
    elib_location The URI of the ELIB

    Returns
    -------
    A pandas `DataFrame` with spectral information for all entries in the ELIB; this is effectively
    just the results of calling `pandas.read_sql("entries", sqlite3.connect(elib_location)`.

    **In most cases it is preferred to call `read_encyclopedia_elib` to get a more user-friendly,
    Spark-based dataset object.**

    The semantics of the set of spectra and peaks depend on hwo the ELIB is created. For converted
    spectral libraries (DLIB extension) or exported chromatogram libraries this will be the set of
    library entries. For single-file ELIBs this will be the set of IDs at the FDR threshold, with
    unrefined or lightly-refined fragment ions. For quantitative ELIBs (created while exporting
    combined quantiative reports) the semantics are unclear.
    """
    raise NotImplementedError("TODO")


def get_peptide_for_psmid(
    psmid: str,
    pep_patt: _Union[str, _re.Pattern] = r":(?:decoy)?([^:]+)\+(\d+)$",
) -> _Dict[str, _Any]:
    """
    Parse an EncyclopeDIA PSMId string to get the (modified_ peptide sequence and charge.

    Parameters
    ----------
    psmid: The PSMId string
    pep_patt: Customizable parsing pattern; defaults to recognize `:[decoy]<seq>+<charge>`. Must
              recognize the sequence as capture group 1 and the charge as group 2.

    Returns
    -------
    {"sequence": "<modseq_str>", "charge": <charge_int>}
    """
    if not isinstance(pep_patt, _re.Pattern):
        pep_patt = _re.compile(pep_patt)

    m = pep_patt.search(psmid)

    return {
        "sequence": m.group(1),
        "charge": int(m.group(2)),
    }


get_peptide_for_psmid_udf = _fns.udf(
    get_peptide_for_psmid,
    returnType=_typ.StructType(
        [
            _typ.StructField("sequence", _typ.StringType()),
            _typ.StructField("charge", _typ.IntegerType()),
        ]
    ),
)
get_peptide_for_psmid_udf.__doc__ = (
    "UDF-decorated function:\n\n" + get_peptide_for_psmid.__doc__
)
