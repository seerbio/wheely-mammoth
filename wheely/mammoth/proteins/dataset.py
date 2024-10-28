"""The :py:class:`ProteinDataset` class is used to define a collection of
protein identifications.
"""

import logging as _logging

import pyspark.sql
from pyspark.sql.functions import (
    col as _col,
)

from ..utils import listify

LOGGER = _logging.getLogger(__name__)


class ProteinDataset:
    """A collection of protein or protein group IDs backed by a :py:class:`pyspark.sql.DataFrame`

    Parameters
    ----------
    data : pyspark.sql.DataFrame
        A :py:class:`pyspark.sql.DataFrame` of proteins.
    protein_column : str
        The column that defines a unique protein. In typical use, this should be the set of protein accessions within
        a group; these may be shared between groups when using a grouping that does not exclusively assign proteins to
        groups. You may specify a string- or list-valued column. When it contains multiple identifiers, ensure you
        specify a protein_delim or use a list-valued column.
    target_column : str
        The column that indicates whether a PSM is a target or a decoy. This
        column should be boolean, where :code:`True` indicates a target and
        :code:`False` indicates a decoy.
    score_columns : str or tuple of str, optional
        One or more columns that indicate scores by which crema can rank PSMs.
    protein_delim : str (optional)
        The string delimiter that is needed to separate multiple proteins found
        in the protein column. Not required if `protein_column` contains only a
        single identifier for each protein group, or if `protein_column` is
        list-valued.

    Attributes
    ----------
    columns : list of str
    data : pyspark.sql.DataFrame
    proteins : pyspark.sql.Column
    scores : pyspark.sql.DataFrame
    targets : pyspark.sql.Column
    protein_delim : str
    """

    def __init__(
        self,
        data: pyspark.sql.DataFrame,
        protein_column,
        target_column,
        score_columns,
        protein_delim=None,
    ):
        """Initialize a PsmDataset object."""
        self._data = data
        self._protein_column = protein_column
        self._target_column = target_column
        self._score_columns = listify(score_columns)
        self._protein_delim = protein_delim

    def with_data(self, data, **kwargs):
        """
        Return a new :py:class:`wheely.mammoth.dataset.ProteinDataset` backed
        by `data` but otherwise identical to this dataset. Optionally, any
        arguments accepted by `ProteinDataset()` can be passed as keywords and
        will override the value from this dataset.
        This permits mutating the data (e.g. to filter it), or altering the semantics
        of the dataset.
        """
        return type(self)(
            data,
            **dict(
                dict(
                    protein_column=self.protein_column,
                    target_column=self.target_column,
                    score_columns=self.score_columns,
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
            self.protein_column,
            self.target_column,
            *self.score_columns,
        ]
        return cols

    @property
    def data(self):
        """The collection of PSMs as a :py:class:`pyspark.sql.DataFrame`."""
        return self._data

    @property
    def proteins(self):
        """The proteins as a :py:class:`pyspark.sql.Column`."""
        return pyspark.sql.functions.col(self.protein_column)

    @property
    def targets(self):
        """The PSM target/decoy column as a :py:class:`pyspark.sql.Column`"""
        return pyspark.sql.functions.col(self.target_column)

    @property
    def scores(self):
        """The PSM scores as a :py:class:`pyspark.sql.DataFrame`"""
        return self.data.select(*self.score_columns)

    @property
    def protein_column(self):
        """The name of the column giving protein information, or `None`."""
        return self._protein_column

    @property
    def target_column(self):
        """The name of the column giving target/decoy information."""
        return self._target_column

    @property
    def score_columns(self):
        """The list of columns giving scores."""
        return self._score_columns

    @property
    def protein_delim(self) -> str:
        """The delimiter to split protein IDs as a string, or `None`."""
        return self._protein_delim


class ProteinConfidenceDataset(ProteinDataset):
    """
    A :py:class:`wheely.mammoth.ProteinDataset` with additional information about
    statistical significance.

    Parameters
    ----------
    qvalue_column: str
        The name of the column giving protein _q_-values.
    errprob_column: str, optional
        The name of the column giving posterior error probabilities (PEPs), or `None` if no such column is present.
    pi0: float, optional
        The estimated pi_0 value for the dataset. May be `None` or `numpy.nan` if no such
        value was estimated for the dataset.
    """

    def __init__(
        self,
        data: pyspark.sql.DataFrame,
        protein_column,
        target_column,
        score_columns,
        qvalue_column,
        errprob_column=None,
        protein_delim=None,
        pi0=None,
    ):
        self._qvalue_column = qvalue_column
        self._errprob_column = errprob_column
        self._pi0 = pi0
        super().__init__(
            data,
            protein_column=protein_column,
            target_column=target_column,
            score_columns=score_columns,
            protein_delim=protein_delim,
        )

    def with_data(self, data, **kwargs):
        """
        Return a new :py:class:`wheely.mammoth.dataset.ProteinConfidenceDataset` backed
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
