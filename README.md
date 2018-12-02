Dask Azure Blob FileSystem
==========================

[![Build Status](https://travis-ci.org/manish/dask-azureblobfs.svg?branch=master)](https://travis-ci.org/manish/dask-azureblobfs)
[![Documentation Status](https://readthedocs.org/projects/dask-azureblobfs/badge/?version=latest)](https://dask-azureblobfs.readthedocs.io/en/latest/?badge=latest)

Features
--------

* Supports dask when your data files are stored in the cloud.

  * Import `DaskAzureBlobFileSystem`

  * Use `abfs://` as protocol prefix and you are good to do.

* For authentication, please read more on [Usage](https://dask-azureblobfs.readthedocs.io/en/latest/usage.html).

* Support for key-value storage which is backed by azure storage. Create an instance of `AzureBlobMap`


Install
--------

Using pip

`pip install dask-azureblobfs`

Or using pipenv from git

`pipenv install -e git+https://github.com/manish/dask-azureblobfs#egg=dask-azureblobfs`

Check more on [Installing](https://dask-azureblobfs.readthedocs.io/en/latest/installation.html#)


Documentation
--------------

Official Documentation available on [Read the Docs](https://dask-azureblobfs.readthedocs.io/en/latest/index.html)


Contributing
-------------

We support `Python 2.7`, `Python 3.6` and `Python 3.7` and use
`tox` for running tests on different versions.

Read more on how to run and verify it using docker container - which is based on python 3.6
