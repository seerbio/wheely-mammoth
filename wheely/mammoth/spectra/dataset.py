"""
`wheely.mammoth.spectra.dataset` -- interface for  spectral information
"""

from typing import (
    Iterable as _Iterable,
    Protocol as _Protocol,
    runtime_checkable as _runtime_checkable,
)

from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
    types as _types,
)

from ..utils import listify as _listify


@_runtime_checkable
class PrecursorDataset(_Protocol):
    """
    A collection of information about MS precursor ions, backed by a
    :py:class:`pyspark.sql.DataFrame`

    To easily implement this protocol, extend `PrecursorDatasetBase`.

    Attributes
    ----------
    columns : list of str
    data : pyspark.sql.DataFrame
    spectra : pyspark.sql.DataFrame
    charges : pyspark.sql.Column
    mzs : pyspark.sql.Column
    rts : pyspark.sql.Column
    """

    data: _DataFrame

    spectra: _DataFrame

    charges: _Column
    mzs: _Column
    rts: _Column

    columns: _Iterable[str]
    spectrum_columns: _Iterable[str]

    charge_column: str
    mz_column: str
    rt_column: str


class PrecursorDatasetBase(PrecursorDataset):
    def __init__(
        self,
        psms: _DataFrame,
        spectrum_columns,
        charge_column,
        mz_column,
        rt_column,
    ):
        """
        Parameters
        ----------
        data : pyspark.sql.DataFrame
            A :py:class:`pyspark.sql.DataFrame` with one row per spectrum.
        spectrum_columns : str or tuple of str
            One or more columns that together define a unique mass spectrum.
        charge_column : str
            The name of a column giving the precursor charge of the spectrum.
        mz_column : str
            The name of a column giving the precursor m/z of the spectrum.
        rt_column : str
            The name of a column giving the spectrum's retention time, in an arbitrary scale.
        """
        self._data = psms
        self._spectrum_columns = _listify(spectrum_columns)
        self._charge_column = charge_column
        self._mz_column = mz_column
        self._rt_column = rt_column

    def with_data(self, data, **kwargs):
        """
        Return a new :py:class:`SpectraDataset` backed by `data` but otherwise identical
        to this dataset. Optionally, any arguments accepted by `SpectraDataset()` can be
        passed as keywords and will override the value from this dataset.
        This permits mutating the data (e.g. to filter it), or altering the semantics
        of the dataset.
        """
        return type(self)(
            data,
            **dict(
                dict(
                    spectrum_columns=self.spectrum_columns,
                    charge_column=self.charge_column,
                    mz_column=self.mz_column,
                    rt_column=self.rt_column,
                ),
                **kwargs,
            ),
        )

    @property
    def columns(self):
        """
        The columns of the :py:class:`pyspark.sql.DataFrame` that have defined
        semantics in this dataset. Note that additional columns may be available
        and will be preserved in the backing dataframe.
        """
        return [
            *self.spectrum_columns,
            self.charge_column,
            self.mz_column,
            self.rt_column,
        ]

    @property
    def data(self):
        """The collection of PSMs as a :py:class:`pyspark.sql.DataFrame`."""
        return self._data

    @property
    def spectra(self):
        """The mass spectrum identifiers as a :py:class:`pyspark.sql.DataFrame`."""
        return self.data.select(self.spectrum_columns)

    @property
    def charges(self):
        """The charges as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.charge_column)

    @property
    def mzs(self):
        """The m/z values as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.mz_column)

    @property
    def rts(self):
        """The RTs as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.rt_column)

    @property
    def peaklists(self):
        """The peaklists as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.peaklist_column)

    @property
    def spectrum_columns(self):
        """The names of the columns giving spectrum information."""
        return self._spectrum_columns

    @property
    def charge_column(self):
        """The name of the column giving charge information."""
        return self._charge_column

    @property
    def mz_column(self):
        """The name of the column giving m/z information."""
        return self._mz_column

    @property
    def rt_column(self):
        """The name of the column giving RT information."""
        return self._rt_column


@_runtime_checkable
class SpectraDataset(_Protocol):
    """
    A collection of spectral information backed by a :py:class:`pyspark.sql.DataFrame`

    To easily implement this protocol, extend `SpectraDatasetBase`.

    Attributes
    ----------
    columns : list of str
    data : pyspark.sql.DataFrame
    spectra : pyspark.sql.DataFrame
    charges : pyspark.sql.Column
    mzs : pyspark.sql.Column
    rts : pyspark.sql.Column
    peaklists : pyspark.sql.Column
    """

    data: _DataFrame

    spectra: _DataFrame

    charges: _Column
    mzs: _Column
    rts: _Column
    peaklists: _Column

    columns: _Iterable[str]
    spectrum_columns: _Iterable[str]

    charge_column: str
    mz_column: str
    rt_column: str
    peaklist_column: str


class SpectraDatasetMixin:
    def __init__(
        self,
        peaklist_column,
    ):
        """
        Parameters
        ----------
        peaklist_column :  str
            The name of a column containing structured information in the following format.
            Utilities are provided for conversion to/from other formats using Spark.

            The format should be a list of (m/z, intensity) pairs, as a single column. The
            schema should be `PeaklistType`, or equivalent to:

            ```python
            ArrayType(ArrayType(DoubleType()))
            ```

            Each inner array must have at least two items (m/z, then intensity). Any items beyond the
            first two will be ignored.
        """
        self._peaklist_column = peaklist_column

    @property
    def peaklist_column(self):
        """The name of the column giving peaklist information."""
        return self._peaklist_column


class SpectraDatasetBase(
    SpectraDatasetMixin, PrecursorDatasetBase, SpectraDataset
):
    def __init__(
        self,
        psms: _DataFrame,
        spectrum_columns,
        charge_column,
        mz_column,
        rt_column,
        peaklist_column,
    ):
        PrecursorDatasetBase.__init__(
            self,
            psms,
            spectrum_columns,
            charge_column,
            mz_column,
            rt_column,
        )
        SpectraDatasetMixin.__init__(self, peaklist_column)

    @property
    def columns(self):
        """
        The columns of the :py:class:`pyspark.sql.DataFrame` that have defined
        semantics in this dataset. Note that additional columns may be available
        and will be preserved in the backing dataframe.
        """
        return [
            *self.spectrum_columns,
            self.charge_column,
            self.mz_column,
            self.rt_column,
            self.peaklist_column,
        ]


PeaklistType = _types.ArrayType(
    _types.ArrayType(  # length must be >=2; all but first two elements (mz, inten) are ignored
        _types.DoubleType()
    )
)
