"""
`wheely.mammoth.spectra.parsers.encyclopedia` -- read spectral information from EncyclopeDIA ELIB
files.
"""

import logging as _logging
import os as _os
import re as _re
import struct as _struct
import sys as _sys
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
from ..utils import lists_to_peaklist as _lists_to_peaklist

_logger = _logging.getLogger(__name__)


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
    df = spark.createDataFrame(read_encyclopedia_elib_pandas(elib_location))

    return _wrap_elib_entries(
        df, spectrum_columns=["SourceFile", "PeptideModSeq", "PrecursorCharge"]
    )


def read_encyclopedia_entries(
    psms: _PsmDataset,
    elib_loc_col: _Union[str, _Column] = None,
    elib_loc: _os.PathLike = None,
    file_loc_col: _Union[str, _Column] = None,
    file_loc_patt: _Union[str, _re.Pattern] = None,
    elib_loc_fmt: str = None,
) -> _SpectraDataset:
    """
    Look up and return a table of spectral information for the given (possibly filtered) set of PSMs

    Parameters
    ----------
    elib_loc_col: If neither `elib_loc_col` or `elib_loc` are provided, the (otherwise-ignored)
                  `file_loc_col`, `file_loc_patt` and `elib_loc_fmt` arguments will be passed to
                  `compute_elib_loc` to determine the location of the ELIB.

    Returns
    -------
    A dataset of matching entries from the corresponding ELIB(s), guaranteed to have matching
    `spectrum_columns` for joining back to the original source of PSMs. Note that not all PSMs
    will have matching entries in some cases.
    """
    assert psms.spectrum_columns == [
        "id"
    ], "Unexpected spectrum_columns: " + str(psms.spectrum_columns)

    # 1. Compute the ELIB path for each row
    if elib_loc_col is None:
        if elib_loc:
            _logger.info("Will use ELIB location %s", elib_loc)

            elib_loc_col = _fns.lit(elib_loc)
        else:
            elib_loc_col = compute_elib_loc(
                file_loc_col, file_loc_patt, elib_loc_fmt
            )
    elif not isinstance(elib_loc_col, _Column):
        _logger.info("Taking ELIB locations from column: %s", elib_loc_col)

        elib_loc_col = _fns.col(elib_loc_col)

    # 2. Collect distinct paths
    elib_paths = (
        psms.data.select(elib_loc_col.alias("__elib_path"))
        .dropDuplicates()
        .toPandas()["__elib_path"]
        .values
    )

    _logger.info("Found %d ELIB locations", len(elib_paths))
    _logger.debug("ELIBs: %s", ", ".join(elib_paths))

    # 3. Read ELIBs with Spark
    entries = _read_elibs_rdd_pandas(elib_paths, spark=psms.data.sparkSession)

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Read %d entries from %d ELIBs", entries.count(), len(elib_paths)
        )

    # 4. Join to original spectrum identifiers
    spectral_df = (
        psms.data.select(
            *psms.spectrum_columns,
            elib_loc_col.alias("__elib_path"),
            # Parse peptide information from PSMId strings (as join keys)
            _fns.udf(
                get_peptide_for_psmid,
                returnType=get_peptide_for_psmid.returnType,
            )(_fns.col("id")).alias("peptide"),
        )
        .alias("psm")
        .join(
            entries.alias("entry"),  # Take ALL columns (for now)
            how="inner",  # NOTE: drops PSMs without entries
            on=(
                (
                    _fns.col("psm.__elib_path")
                    == _fns.col("entry.elib_location")
                )
                & (
                    _fns.col("psm.peptide.sequence")
                    == _fns.col("entry.PeptideModSeq")
                )
                & (
                    _fns.col("psm.peptide.charge")
                    == _fns.col("entry.PrecursorCharge")
                )
            ),
        )
    )

    if _logger.isEnabledFor(_logging.DEBUG):
        _logger.debug(
            "Joined %d entries to PSM identifiers", spectral_df.count()
        )

    # 5. Build and return dataset object
    return _wrap_elib_entries(
        # Here we select only the columns we'd like to return
        spectral_df.select(
            *psms.spectrum_columns,
            *[
                _fns.col(f"entry.{c}").alias(c)
                for c in [
                    "PrecursorCharge",
                    "PrecursorMz",
                    "RTInSeconds",
                    # Include these columns so _wrap_elib_entries parses them for us
                    # (they will be dropped from the returned DataFrame).
                    "MassArray",
                    "IntensityArray",
                    "CorrelationArray",
                ]
            ],
        ),
        spectrum_columns=psms.spectrum_columns,
    )


def compute_elib_loc(
    file_loc_col: _Union[str, _Column] = None,
    file_loc_patt: _Union[str, _re.Pattern] = None,
    elib_loc_fmt: str = None,
) -> _Column:
    """
    Return a column that computes the ELIB location for each PSM using the given column and regex/pattern.

    Parameters
    ----------
    file_loc_col: the column giving information about the file location (default: "filename")
    file_loc_patt: A regex that parses the location value (default: r"^(?:file://)?(.+)\.features\.txt$")
                   The pattern's `search()` method will be invoked, so patterns must be properly
                   anchored to match the desired element(s) of the value(s).
    elib_loc_fmt: A format string that uses capture groups from the regex (default: "{1:s}.elib")
                  The first argument (index 0) will be the whole match, the remaining arguments will
                  be the individual capture groups of the regex.

    Returns
    -------
    A column giving the location of the corresponding ELIB for each row.
    """
    file_loc_col = _fns.col(file_loc_col or "filename")

    if not isinstance(file_loc_patt, _re.Pattern):
        file_loc_patt = _re.compile(
            file_loc_patt
            or (
                r"^(?:file:///)?(.+)\.features\.txt$"
                if _sys.platform.startswith("win")
                else r"^(?:file://)?(.+)\.features\.txt$"
            )
        )

    elib_loc_fmt = elib_loc_fmt or "{1:s}.elib"

    _logger.info(
        "Will compute ELIB location from the `%s` column", file_loc_col
    )
    _logger.debug("file_loc_patt=%s", file_loc_patt)
    _logger.debug("elib_loc_fmt=%s", elib_loc_fmt)

    def _compute_elib_loc(file_loc):
        match = file_loc_patt.search(file_loc)
        return elib_loc_fmt.format(match.group(), *match.groups())

    return _fns.udf(_compute_elib_loc, returnType="string")(file_loc_col)


def _wrap_elib_entries(
    df,
    spectrum_columns=["SourceFile", "PeptideModSeq", "PrecursorCharge"],
    charge_column="PrecursorCharge",
    mz_column="PrecursorMz",
    rt_column="RTInSeconds",
    peaklist_column=None,
):
    if not peaklist_column:
        df = df.withColumn(
            "peaklist",
            _lists_to_peaklist(
                _fns.udf(
                    decode_double_array,
                    returnType=decode_double_array.returnType,
                )("MassArray"),
                _fns.udf(
                    decode_float_array,
                    returnType=decode_float_array.returnType,
                )("IntensityArray"),
                _fns.udf(
                    decode_float_array,
                    returnType=decode_float_array.returnType,
                )("CorrelationArray"),
            ),
        ).drop("MassArray", "IntensityArray", "CorrelationArray")

        peaklist_column = "peaklist"

    return _SpectraDataset(
        df,
        spectrum_columns=spectrum_columns,
        charge_column=charge_column,
        mz_column=mz_column,
        rt_column=rt_column,
        peaklist_column=peaklist_column,
    )


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
    combined quantitative reports) the semantics are unclear.
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

    _logger.info("Reading entries from %s", elib_uri)

    try:
        con = _sqlite_conn(elib_uri, uri=True)
    except _sqlite_err as e:
        raise FileNotFoundError("Error connecting to URI " + elib_uri) from e
    else:
        with con:
            df = _pd.read_sql("SELECT * FROM entries;", con)

        df["elib_location"] = elib_location

    _logger.info("Read %d entries from %s", len(df), elib_location)

    return df


def _read_elibs_rdd_pandas(elib_paths, spark=None):
    if not spark:
        spark = _SparkSession.Builder.getOrCreate()

    _logger.info(
        "Will read entries from %d ELIBs with Pandas", len(elib_paths)
    )

    # Use the approach of https://hdfgroup.org/2015/04/putting-some-spark-into-hdf-eos/
    files_rdd = spark.sparkContext.parallelize(elib_paths, len(elib_paths))
    psms_rdd = files_rdd.flatMap(
        lambda f: read_encyclopedia_elib_pandas(f).itertuples(index=False)
    )

    return spark.createDataFrame(psms_rdd)


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


get_peptide_for_psmid.returnType = _typ.StructType(
    [
        _typ.StructField("sequence", _typ.StringType()),
        _typ.StructField("charge", _typ.IntegerType()),
    ]
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
