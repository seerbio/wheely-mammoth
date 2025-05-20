"""The :py:class:`PsmDataset` class is used to define a collection of
peptide-spectrum matches.
"""

import logging

from typing import (
    Iterable as _Iterable,
    List as _List,
    Protocol as _Protocol,
    runtime_checkable as _runtime_checkable,
)

import pyspark.sql
from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
)
from pyspark.sql.functions import (
    col as _col,
)

from .utils import listify

LOGGER = logging.getLogger(__name__)


class PsmDataset:
    """A collection of peptide-spectrum matches (PSMs) backed by a :py:class:`pyspark.sql.DataFrame`

    Parameters
    ----------
    psms : pyspark.sql.DataFrame
        A :py:class:`pyspark.sql.DataFrame` of PSMs.
    target_column : str
        The column that indicates whether a PSM is a target or a decoy. This
        column should be boolean, where :code:`True` indicates a target and
        :code:`False` indicates a decoy.
    spectrum_columns : str or tuple of str
        One or more columns that together define a unique mass spectrum.
    score_columns : str or tuple of str, optional
        One or more columns that indicate scores by which crema can rank PSMs.
    peptide_column : str
        The column that defines a unique peptide. Modifications should be
        indicated either in square brackets :code:`[]` or parentheses
        :code:`()`. The exact modification format within these entities does
        not matter, so long as it is consistent.
    charge_column: str (optional)
        The column that specifies the PSM's precursor charge state. If `None`
        it's assumed that the dataset does not have charge state information.
    protein_columns : str (optional)
        The column that defines a unique protein.
    protein_delim : str (optional)
        The string delimiter that is needed to separate multiple proteins found
        in the protein column.

    Attributes
    ----------
    columns : list of str
    data : pyspark.sql.DataFrame
    spectra : pyspark.sql.DataFrame
    peptides : pyspark.sql.DataFrame
    proteins : pyspark.sql.DataFrame
    protein_delim : str
    """

    def __init__(
        self,
        psms: pyspark.sql.DataFrame,
        target_column,
        score_columns,
        spectrum_columns,
        peptide_column,
        *_args,
        charge_column=None,
        protein_column=None,
        protein_delim=None,
    ):
        """Initialize a PsmDataset object."""
        self._data = psms
        self._target_column = target_column
        self._score_columns = listify(score_columns)
        self._spectrum_columns = listify(spectrum_columns)
        self._peptide_column = peptide_column
        self._charge_column = charge_column
        self._protein_column = protein_column
        self._protein_delim = protein_delim

    def with_data(self, data, **kwargs):
        """
        Return a new :py:class:`wheely.mammoth.dataset.PsmDataset` backed
        by `data` but otherwise identical to this dataset. Optionally, any
        arguments accepted by `PsmDataset()` can be passed as keywords and
        will override the value from this dataset.
        This permits mutating the data (e.g. to filter it), or altering the semantics
        of the dataset's peptide/spectrum grouping, decoy definition, etc.
        """
        return type(self)(
            data,
            **dict(
                dict(
                    target_column=self.target_column,
                    score_columns=self.score_columns,
                    spectrum_columns=self.spectrum_columns,
                    peptide_column=self.peptide_column,
                    charge_column=self.charge_column,
                    protein_column=self.protein_column,
                    protein_delim=self.protein_delim,
                ),
                **kwargs,
            ),
        )

    @property
    def columns(self):
        """
        The columns of the PSM :py:class:`pyspark.sql.DataFrame` that have defined
        semantics in this dataset. Note that additional columns may be available
        and will be preserved in the backing dataframe.
        """
        cols = [
            self.target_column,
            *self.score_columns,
            *self.spectrum_columns,
            self.peptide_column,
        ]
        if self.charge_column is not None:
            cols.append(self.charge_column)
        if self.protein_column is not None:
            cols.append(self.protein_column)
        return cols

    @property
    def data(self):
        """The collection of PSMs as a :py:class:`pyspark.sql.DataFrame`."""
        return self._data

    @property
    def targets(self):
        """The PSM target/decoy column as a :py:class:`pyspark.sql.Column`"""
        return pyspark.sql.functions.col(self.target_column)

    @property
    def scores(self):
        """The PSM scores as a :py:class:`pyspark.sql.DataFrame`"""
        return self.data.select(*self.score_columns)

    @property
    def spectra(self):
        """The mass spectrum identifiers as a :py:class:`pyspark.sql.DataFrame`."""
        return self.data.select(*self.spectrum_columns)

    @property
    def peptides(self):
        """The peptides as a :py:class:`pyspark.sql.Column`."""
        return pyspark.sql.functions.col(self.peptide_column)

    @property
    def charges(self):
        """The charges as a :py:class:`pyspark.sql.Column`, or `None`"""
        return (
            pyspark.sql.functions.col(self.charge_column)
            if self.charge_column
            else None
        )

    @property
    def proteins(self):
        """The proteins as a :py:class:`pyspark.sql.Column`."""
        return pyspark.sql.functions.col(self.protein_column)

    @property
    def target_column(self):
        """The name of the column giving target/decoy information."""
        return self._target_column

    @property
    def score_columns(self):
        """The list of columns giving scores."""
        return self._score_columns

    @property
    def spectrum_columns(self):
        """The names of the columns giving spectrum information."""
        return self._spectrum_columns

    @property
    def peptide_column(self):
        """The name of the column giving peptide information."""
        return self._peptide_column

    @property
    def charge_column(self):
        """The name of the column giving charge information, or `None`"""
        return self._charge_column

    @property
    def protein_column(self):
        """The name of the column giving protein information, or `None`."""
        return self._protein_column

    @property
    def protein_delim(self) -> str:
        """The delimiter to split protein IDs as a string, or `None`."""
        return self._protein_delim


class ConfidenceDataset(PsmDataset):
    """
    A :py:class:`wheely.mammoth.PsmDataset` with additional information about
    statistical significance.

    Parameters
    ----------
    qvalue_column: str
        The name of the column giving PSM/peptide _q_-values.
    errprob_column: str, optional
        The name of the column giving posterior error probabilities (PEPs), or `None` if no such column is present.
    pi0: float, optional
        The estimated pi_0 value for the dataset. May be `None` or `numpy.nan` if no such
        value was estimated for the dataset.
    """

    def __init__(
        self,
        psms: pyspark.sql.DataFrame,
        target_column,
        spectrum_columns,
        score_columns,
        peptide_column,
        qvalue_column,
        *_args,
        charge_column=None,
        pi0=None,
        protein_column=None,
        protein_delim=None,
        errprob_column=None,
    ):
        self._qvalue_column = qvalue_column
        self._errprob_column = errprob_column
        self._pi0 = pi0
        super().__init__(
            psms,
            target_column=target_column,
            score_columns=score_columns,
            spectrum_columns=spectrum_columns,
            peptide_column=peptide_column,
            charge_column=charge_column,
            protein_column=protein_column,
            protein_delim=protein_delim,
        )

    def with_data(self, data, **kwargs):
        """
        Return a new :py:class:`wheely.mammoth.dataset.ConfidenceDataset` backed
        by `data` but otherwise identical to this dataset. Optionally, any
        arguments accepted by `ConfidenceDataset()` can be passed as keywords and
        will override the value from this dataset.
        This permits mutating the data (e.g. to filter it), or altering the semantics
        of the dataset's peptide/spectrum grouping, decoy definition, etc.
        """
        return super().with_data(
            data,
            **dict(
                dict(
                    qvalue_column=self.qvalue_column,
                    errprob_column=self.errprob_column,
                    pi0=self.pi0,
                ),
                **kwargs,
            ),
        )

    @property
    def columns(self):
        """
        All the columns understood in this dataset.
        """
        return [
            *super().columns,
            self.qvalue_column,
            *[c for c in [self.errprob_column] if c is not None],
        ]

    @property
    def qvalues(self):
        """
        The PSM/peptide _q_-values as a :py:class:`pyspark.sql.Column`.
        """
        return pyspark.sql.functions.col(self.qvalue_column)

    @property
    def errprobs(self):
        """
        The PSM/peptide posterior error probabilities (PEPs) as a :py:class:`pyspark.sql.Column`.
        """
        return (
            pyspark.sql.functions.col(self.errprob_column)
            if self.errprob_column
            else None
        )

    @property
    def qvalue_column(self):
        """
        The name of the column giving PSM/peptide _q_-values.
        """
        return self._qvalue_column

    @property
    def errprob_column(self):
        """
        The name of the column giving PSM/peptide posterior error probabilities (PEPs).
        """
        return self._errprob_column

    @property
    def pi0(self):
        """
        The estimated pi_0 value for the dataset, or `None`/`numpy.nan` if no such value was
        estimated.
        """
        return self._pi0


@_runtime_checkable
class IntensityDataset(_Protocol):
    """
    Attributes
    ----------
    data : pyspark.sql.DataFrame
    columns : list of str
    samples : pyspark.sql.Column
    intensities : pyspark.sql.Column
    sample_column : str
    intensity_column : str
        The name of the column containing primary intensity values.
    intensity_columns : str
        The names of all columns containing intensity values.
        This permits reporting multiple types of intensity.
    """

    data: _DataFrame

    columns: _Iterable[str]

    samples: _Column

    intensities: _Column

    sample_column: str

    intensity_column: str

    intensity_columns: _List[str]


class IntensityDatasetMixin(IntensityDataset):
    def __init__(
        self,
        sample_column: str,
        intensity_column: str,
        intensity_columns: _Iterable[str] = None,
    ):
        """
        Parameters
        ----------
        sample_column: str
            The name of the column giving sample identifiers.
        intensity_column :  str
            The name of a column containing intensity values.
        intensity_columns : [str], optional
            The names of all columns containing intensity values.
            If `None`, only the `intensity_column` is used.
        """
        self._sample_column = sample_column
        self._intensity_column = intensity_column
        self._intensity_columns = [
            self._intensity_column,
            *[
                c
                for c in intensity_columns or []
                if c != self._intensity_column
            ],
        ]

    @property
    def samples(self) -> _Column:
        """A :py:class:`pyspark.sql.Column` of sample identifiers."""
        return _col(self._sample_column)

    @property
    def intensities(self) -> _Column:
        """The intensities as a :py:class:`pyspark.sql.Column`."""
        return _col(self.intensity_column)

    @property
    def sample_column(self) -> str:
        """The name of the column giving sample identifiers."""
        return self._sample_column

    @property
    def intensity_column(self) -> str:
        """The name of the column containing intensities."""
        return self._intensity_column

    @property
    def intensity_columns(self) -> str:
        """The name of the column containing intensities."""
        return [*self._intensity_columns]


class PsmIntensityDataset(IntensityDatasetMixin, PsmDataset):
    def __init__(
        self,
        psms: pyspark.sql.DataFrame,
        target_column,
        score_columns,
        spectrum_columns,
        peptide_column,
        sample_column: str,
        intensity_column: str,
        *_args,
        intensity_columns: _Iterable[str] = None,
        charge_column=None,
        protein_column=None,
        protein_delim=None,
    ):
        if _args:
            raise TypeError("Additional positional arguments are unsupported!")

        PsmDataset.__init__(
            self,
            psms=psms,
            target_column=target_column,
            score_columns=score_columns,
            spectrum_columns=spectrum_columns,
            peptide_column=peptide_column,
            charge_column=charge_column,
            protein_column=protein_column,
            protein_delim=protein_delim,
        )
        IntensityDatasetMixin.__init__(
            self,
            sample_column=sample_column,
            intensity_column=intensity_column,
            intensity_columns=intensity_columns,
        )

    @property
    def columns(self):
        """
        The columns of the :py:class:`pyspark.sql.DataFrame` that have defined
        semantics in this dataset. Note that additional columns may be available
        and will be preserved in the backing dataframe.
        """
        return [
            *super().columns,
            self.sample_column,
            *self.intensity_columns,
        ]

    def with_data(self, data, **kwargs):
        return super().with_data(
            data,
            **dict(
                dict(
                    sample_column=self.sample_column,
                    intensity_column=self.intensity_column,
                    intensity_columns=self.intensity_columns,
                ),
                **kwargs,
            ),
        )


class PsmIntensityConfidenceDataset(ConfidenceDataset, IntensityDatasetMixin):
    def __init__(
        self,
        psms: pyspark.sql.DataFrame,
        target_column: str,
        score_columns: _List[str],
        spectrum_columns: _List[str],
        peptide_column: str,
        qvalue_column: str,
        sample_column: str,
        intensity_column: str,
        *_args,
        intensity_columns: _Iterable[str] = None,
        charge_column: str = None,
        protein_column: str = None,
        protein_delim: str = None,
        errprob_column: str = None,
        pi0: float = None,
    ):
        if _args:
            raise TypeError("Additional positional arguments are unsupported!")

        ConfidenceDataset.__init__(
            self,
            psms=psms,
            target_column=target_column,
            score_columns=score_columns,
            spectrum_columns=spectrum_columns,
            peptide_column=peptide_column,
            qvalue_column=qvalue_column,
            charge_column=charge_column,
            protein_column=protein_column,
            protein_delim=protein_delim,
            errprob_column=errprob_column,
            pi0=pi0,
        )
        IntensityDatasetMixin.__init__(
            self,
            sample_column=sample_column,
            intensity_column=intensity_column,
            intensity_columns=intensity_columns,
        )

    @property
    def columns(self):
        """
        The columns of the :py:class:`pyspark.sql.DataFrame` that have defined
        semantics in this dataset. Note that additional columns may be available
        and will be preserved in the backing dataframe.
        """
        return [
            *super().columns,
            self.sample_column,
            *self.intensity_columns,
        ]

    def with_data(self, data, **kwargs):
        return super().with_data(
            data,
            **dict(
                dict(
                    sample_column=self.sample_column,
                    intensity_column=self.intensity_column,
                    intensity_columns=self.intensity_columns,
                ),
                **kwargs,
            ),
        )
