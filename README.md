# PetalData Python Library

The PetalData Python library provides convenient access to the [PetalData](https://petaldata.app) API from applications written in the Python language. It includes a set of convenience methods to handle common data access patterns like downloading data from a resource and incrementally updating the dataset.

## Installation

```
pip install --upgrade petaldata
```

## Requirements

Python 3.4+

## Usage

The following configuration is required:

```python
import petaldata
petaldata.storage.Local.dir = os.getenv("CACHE_DIR") # downloads are saved to this directory
```

## Example Usage

See the [examples directory](/examples).

## Releasing

Bump the version number in `setup.py`. Then generate the distribution archive and upload the archive:

```
rm dist/*
python setup.py sdist bdist_wheel
python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

## Adding a Dataset

* Create a `petaldata/dataset/[CLOUD_APP]` directory
* Create a `petaldata/dataset/[CLOUD_APP]/[DATASET_NAME (PLURAL)].py` file.
  * Create a class that inherits from `petaldata.dataset.abstract.Dataset`. See `petaldata.dataset.stripe.Invoices` for an example.
* Create a `petaldata/dataset/[CLOUD_APP]/__init__.py` file. 
  * Import the dataset created above. 
  * Add config variables needed to use the `[CLOUD_APP] API (like an `API KEY`).
* Add `from petaldata.dataset import [CLOUD_APP]` to `petaldata/__init__.py`

