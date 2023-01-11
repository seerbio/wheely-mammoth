"""
`wheely.mammoth.parsers.encyclopedia`: read EncyclopeDIA datasets
"""
import pyspark.sql
import pyspark.sql.functions

from .. import PsmDataset
from ..utils import listify


def read_encyclopedia_features(tsv_files, spark=None):
    """Read peptide-spectrum matches (PSMs) from EncyclopeDIA tab-delimited "feature" files,
    which contain raw scored features and decoys from searches of individual MS runs.
    This format is nearly identical to the plaintext Percolator "PIN" tabular format.

    Parameters
    ----------
    tsv_files : str or tuple of str
        Paths or URIs specifying a collection of PSMs in the EncyclopeDIA tab-delimited-feature
        format.
    spark : SparkSession (optional)
        If `None`, creates a default session.

    Returns
    -------
    PsmDataset
        A :py:class:`wheely.mammoth.dataset.PsmDataset` object containing the parsed PSMs.
    """
    if not spark:
        spark = pyspark.sql.SparkSession.builder.getOrCreate()

    dataset = (
        spark.read
        .format("csv")
        .load(
            [str(p) for p in listify(tsv_files)],
            sep="\t",
            header=True,
            inferSchema=True
        )
        .withColumn("target", pyspark.sql.functions.col("TD") == 1)
    )

    psms = PsmDataset(
        dataset,
        target_column="target",
        spectrum_columns=["id"],
        score_columns=[
            "primary",
            "xCorrLib",
            "xCorrModel",
            "LogDotProduct",
            "logWeightedDotProduct",
            "sumOfSquaredErrors",
            "weightedSumOfSquaredErrors",
            "numberOfMatchingPeaks",
            "numberOfMatchingPeaksAboveThreshold",
            "averageAbsFragmentDeltaMass",
            "averageFragmentDeltaMasses",
            "isotopeDotProduct",
            "averageAbsParentDeltaMass",
            "averageParentDeltaMass",
            "eValue",
            "deltaRT",
            "numMissedCleavage",
            "pepLength",
        ],
        peptide_column="sequence",
        protein_column="protein",
        protein_delim=";",
    )

    return psms

