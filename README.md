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
petaldata.cache_dir = os.getenv("CACHE_DIR") # downloads are saved to this directory
```

## Example Usage

See the [examples directory](/examples).

## Releasing

Generate the distribution archive, then upload the archive:

```
python setup.py sdist bdist_wheel
python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```
