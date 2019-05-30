# PetalData Python Library

[PetalData](https://petaldata.app) is a uniform API for exporting data science-ready datasets from cloud apps like Stripe, Hubspot, and Metabase.

## Installation

```
pip install --upgrade petaldata
```

## Requirements

Python 3.4+

## Usage

```python
import petaldata
```

## Example Usage

See the [examples directory](https://github.com/petaldata/petaldata-python/tree/master/examples) in the [GitHub repo](https://github.com/petaldata/petaldata-python).

## Documentation

Interactive documentation is at https://petaldata.app/.

## Releasing

Bump the version number in `setup.py`. Then generate the distribution archive and upload the archive:

```
rm dist/*
python setup.py sdist bdist_wheel
python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

## Adding a Dataset

* Create a `petaldata/datasets/[CLOUD_APP]` directory
* Create a `petaldata/datasets/[CLOUD_APP]/[DATASET_NAME (PLURAL)].py` file.
  * Create a class that inherits from `petaldata.datasets.abstract.Dataset`. See `petaldata.datasets.stripe.Invoices` for an example.
* Create a `petaldata/datasets/[CLOUD_APP]/__init__.py` file. 
  * Import the dataset created above. 
  * Add config variables needed to use the `[CLOUD_APP] API (like an `API KEY`).
* Add `from petaldata.datasets import [CLOUD_APP]` to `petaldata/__init__.py`

## Questions

Email support@petaldata.app.