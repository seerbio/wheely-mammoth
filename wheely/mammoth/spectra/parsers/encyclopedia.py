"""
`wheely.mammoth.spectra.parsers.encyclopedia` -- read spectral information from EncyclopeDIA ELIB
files.
"""

import os as _os
import re as _re
import struct as _struct
from sqlite3 import connect as _sqlite_conn, OperationalError as _sqlite_err
from typing import (
    Any as _Any,
    Dict as _Dict,
    Union as _Union,
)
import zlib as _zlib


import pandas as _pd
from pyspark.sql import (
    functions as _fns,
    types as _typ,
    Column as _Column,
    SparkSession as _SparkSession,
)

from ...dataset import PsmDataset as _PsmDataset
from .. import SpectraDataset as _SpectraDataset


def read_encyclopedia_elib(
    elib_location: _os.PathLike, spark: _SparkSession = None
) -> _SpectraDataset:
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


def read_encyclopedia_elib_pandas(
    elib_location: _os.PathLike,
) -> _pd.DataFrame:
    """
    Read a single ELIB and return all its entries.

    Parameters
    ----------
    elib_location
        The path of the ELIB on the local filesystem, or on dbfs (if prefixed by `dbfs:/`).
        If a URI with the file scheme is provided (starting with `file:`) it will be passed directly
        to sqlite, otherwise a URI filename will be built to open the file in readonly mode.

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
    if not isinstance(elib_location, str):
        elib_location = elib_location.__fspath__()

    if elib_location.lower().startswith("dbfs:/"):
        elib_location = "/dbfs" + elib_location[5:]

        if not _os.path.exists(elib_location):
            raise FileNotFoundError("File does not exist: " + elib_location)

        # Open with the immutable flag to avoid locking problems with DBFS
        elib_uri = f"file:{elib_location}?immutable=1"
    elif elib_location.startswith("file:"):
        # URI is already in the file: scheme, pass it directly
        elib_uri = elib_location
    else:
        if not _os.path.exists(elib_location):
            raise FileNotFoundError("File does not exist: " + elib_location)

        elib_uri = f"file:{elib_location}?mode=ro"

    try:
        con = _sqlite_conn(elib_uri, uri=True)
    except _sqlite_err as e:
        raise FileNotFoundError("Error connecting to URI " + elib_uri) from e
    else:
        with con:
            df = _pd.read_sql("SELECT * FROM entries;", con)

        df["elib_location"] = elib_location

    return df


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


get_peptide_for_psmid.returnType = (
    _typ.StructType(
        [
            _typ.StructField("sequence", _typ.StringType()),
            _typ.StructField("charge", _typ.IntegerType()),
        ]
    ),
)


def decode_double_array(comp_bytes):
    return _decode_array("d", comp_bytes)


decode_double_array.returnType = "array<double>"


def decode_float_array(comp_bytes):
    return _decode_array("f", comp_bytes)


decode_float_array.returnType = "array<float>"


def _decode_array(elem_fmt, comp_bytes, big_endian=True):
    bytes = _zlib.decompress(comp_bytes)

    return _struct.unpack(
        (">" if big_endian else "<")
        + elem_fmt * int(len(bytes) / _struct.calcsize(elem_fmt)),
        bytes,
    )
