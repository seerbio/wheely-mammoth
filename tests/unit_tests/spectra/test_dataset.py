"""
These are unit tests for precursor/spectrum dataset classes.
"""

import pandas as pd
import pyspark.sql.functions as fns
import pytest

from wheely.mammoth.spectra import *


@pytest.fixture(
    params=[
        PrecursorDatasetBase,
        # Test that "quoted" column names work
        lambda *args, **kwargs: PrecursorDatasetBase(
            *args,
            **{
                k: (
                    [f"`{c}`" for c in v]
                    if "columns" in k
                    else f"`{v}`" if "column" in k else v
                )
                for k, v in kwargs.items()
            },
        ),
    ]
)
def prec_dataset_type(request):
    return request.param


@pytest.fixture(
    params=[
        SpectraDatasetBase,
        lambda *args, **kwargs: SpectraDatasetBase(
            *args,
            **{
                k: (
                    [f"`{c}`" for c in v]
                    if "columns" in k
                    else f"`{v}`" if "column" in k else v
                )
                for k, v in kwargs.items()
            },
        ),
    ]
)
def spec_dataset_type(request):
    return request.param


def test_prec_dataset_attrs(basic_crux_spark_df, prec_dataset_type):
    """Check the public properties of the dataset object."""
    psms = prec_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
    )

    assert isinstance(psms, PrecursorDataset)

    assert len(psms.spectra.columns) == len(psms.spectrum_columns)
    assert len(psms.data.select(psms.charges).columns) == 1
    assert len(psms.data.select(psms.mzs).columns) == 1
    assert len(psms.data.select(psms.rts).columns) == 1

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
    }


def test_spec_dataset_attrs(basic_crux_spark_df, spec_dataset_type):
    """Check the public properties of the dataset object."""
    psms = spec_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
                "peaklist": fns.array().astype(PeaklistType),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
        peaklist_column="peaklist",
    )

    assert isinstance(psms, PrecursorDataset)
    assert isinstance(psms, SpectraDataset)

    assert len(psms.spectra.columns) == len(psms.spectrum_columns)
    assert len(psms.data.select(psms.charges).columns) == 1
    assert len(psms.data.select(psms.mzs).columns) == 1
    assert len(psms.data.select(psms.rts).columns) == 1
    assert len(psms.data.select(psms.peaklists).columns) == 1

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
        psms.peaklist_column,
    }
