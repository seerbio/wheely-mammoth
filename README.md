<img alt="wheely-mammoth logo" src="./static/wheely-mammoth-logo.png" height="128" align="left" style="margin: 8px">

**wheely-mammoth** is a Python library for reading and handling extremely large
proteomics datasets with PySpark. It provides a simple way to find and describe
datasets for use with other tools.

## Installation  

wheely-mammoth requires Python 3.8+ and can be installed with pip  

```shell
pip install wheely-mammoth
```

## Basic Usage  

Example usage:
```pycon
>>> from wheely.mammoth.parsers import read_encyclopedia_features
>>> ds = read_encyclopedia_features("data/*.features.txt")
>>> type(ds)
<class 'wheely.mammoth.dataset.PsmDataset'>
>>> ds.scores.select(ds.score_columns[0]).describe().toPandas()
  summary             primary
0   count                1770
1    mean  12.408585019887022
2  stddev   3.727298477605115
3     min           6.9876947
4     max            43.78316
```
