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

    dataset = spark.read.format("csv").load(
        [str(p) for p in listify(tsv_files)],
        sep="\t",
        header=True,
        inferSchema=True,
    )

    # Take all columns that aren't the first 3 (PSM info) and the last 1 (protein info)
    score_cols = dataset.columns[3:-2]

    assert all(
        c not in score_cols
        for c in [
            "id",
            "ScanNr",
            "TD",
            "Label",
            "sequence",
            "protein",
            "Proteins",
        ]
    ), f"Score column selection has misbehaved! Got: {score_cols}"

    # Add column giving the name of the file each PSM is read from
    dataset = dataset.withColumn(
        "filename", pyspark.sql.functions.input_file_name()
    )

    # Parse target/decoy label
    tgt_col = "TD" if "TD" in dataset.columns else "Label"
    assert (
        tgt_col in dataset.columns
    ), "Did not find target/decoy label column!"

    dataset = dataset.withColumn(
        "target", pyspark.sql.functions.col(tgt_col) == 1
    )

    psms = PsmDataset(
        dataset,
        target_column="target",
        spectrum_columns=["id"],
        score_columns=score_cols,
        peptide_column="sequence",
        protein_column="protein"
        if "protein" in dataset.columns
        else "Proteins",
        protein_delim=";",
    )

    return psms
