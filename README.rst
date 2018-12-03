==========================
Dask Azure Blob FileSystem
==========================

Azure Blob Storage Backend for Dask

.. image:: https://travis-ci.org/manish/dask-azureblobfs.svg?branch=master
    :target: https://travis-ci.org/manish/dask-azureblobfs

.. image:: https://readthedocs.org/projects/dask-azureblobfs/badge/?version=latest
    :target: https://dask-azureblobfs.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://pyup.io/repos/github/manish/dask-azureblobfs/python-3-shield.svg
     :target: https://pyup.io/repos/github/manish/dask-azureblobfs/
     :alt: Python 3

.. image:: https://pyup.io/repos/github/manish/dask-azureblobfs/shield.svg
     :target: https://pyup.io/repos/github/manish/dask-azureblobfs/
     :alt: Updates

Features
--------

* Supports dask when your data files are stored in the cloud.

  * Import ``DaskAzureBlobFileSystem``

  * Use ``abfs://`` as protocol prefix and you are good to do.

* For authentication, please read more on Usage_.

* Support for key-value storage which is backed by azure storage. Create an instance of ``AzureBlobMap``


Usage
-----

Make the right imports::

    from azureblobfs.dask import DaskAzureBlobFileSystem
    import dask.dataframe as dd

then put all data files in an azure storage container say ``clippy``, then you can read it::

    data = dd.read_csv("abfs://noaa/clippy/weather*.csv")
    max_by_state = data.groupby("states").max().compute()

you would need to set your azure account name in environment variable ``AZURE_BLOB_ACCOUNT_NAME``
(which in our above example is ``noaa``) and the account key in ``AZURE_BLOB_ACCOUNT_KEY``.

If you don't want to use account key and instead want to use SAS, set it in the
environment variable ``AZURE_BLOB_SAS_TOKEN`` along with the connection string in the
environment variable ``AZURE_BLOB_CONNECTION_STRING``.

Installation
------------

Just::

    pip install dask-azureblobfs

or get the development version if you love to live dangerously::

    pip install git+https://github.com/manish/dask-azureblobfs@master#egg=dask-azureblobfs

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Usage: https://dask-azureblobfs.readthedocs.io/en/latest/usage.html
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
