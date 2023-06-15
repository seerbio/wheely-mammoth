"""The :py:class:`PsmDataset` class is used to define a collection of
peptide-spectrum matches.
"""
import logging

import pyspark.sql

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
        protein_column=None,
        protein_delim=None,
    ):
        """Initialize a PsmDataset object."""
        self._data = psms
        self._target_column = target_column
        self._score_columns = listify(score_columns)
        self._spectrum_columns = listify(spectrum_columns)
        self._peptide_column = peptide_column
        self._protein_column = protein_column
        self._protein_delim = protein_delim

        if self.data.isEmpty():
            raise ValueError("No PSMs were provided!")

        # if not self._num_decoys:
        #     raise ValueError("No decoy PSMs were detected.")
        #
        # if not self._num_targets:
        #     raise ValueError("No target PSMs were detected.")

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
        return getattr(self.data, self.target_column)

    @property
    def scores(self):
        """The PSM scores as a :py:class:`pyspark.sql.DataFrame`"""
        return self.data.select(self.score_columns)

    @property
    def spectra(self):
        """The mass spectrum identifiers as a :py:class:`pyspark.sql.DataFrame`."""
        return self.data.select(self.spectrum_columns)

    @property
    def peptides(self):
        """The peptides as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.peptide_column)

    @property
    def proteins(self):
        """The proteins as a :py:class:`pyspark.sql.Column`."""
        return getattr(self.data, self.protein_column)

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
        pi0=None,
        protein_column=None,
        protein_delim=None,
    ):
        self._qvalue_column = qvalue_column
        self._pi0 = pi0
        super().__init__(
            psms,
            target_column=target_column,
            score_columns=score_columns,
            spectrum_columns=spectrum_columns,
            peptide_column=peptide_column,
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
            **kwargs,
            qvalue_column=self.qvalue_column,
            pi0=self.pi0,
        )

    @property
    def columns(self):
        """
        All the columns understood in this dataset.
        """
        return [
            *super().columns,
            self.qvalue_column,
        ]

    @property
    def qvalues(self):
        """
        The PSM/peptide _q_-values as a :py:class:`pyspark.sql.Column`.
        """
        return getattr(self.data, self.qvalue_column)

    @property
    def qvalue_column(self):
        """
        The name of the column giving PSM/peptide _q_-values.
        """
        return self._qvalue_column

    @property
    def pi0(self):
        """
        The estimated pi_0 value for the dataset, or `None`/`numpy.nan` if no such value was
        estimated.
        """
        return self._pi0
