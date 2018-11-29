==========================
Dask Azure Blob FileSystem
==========================

Azure Blob Storage Backend for Dask

.. image:: https://travis-ci.org/manish/dask-azureblobfs.svg?branch=master
    :target: https://travis-ci.org/manish/dask-azureblobfs

Features
--------

* Supports dask when your data files are stored in the cloud.

    * Import `DaskAzureBlobFileSystem`

    * Use `abfs://` as protocol prefix and you are good to do.

* For authentication, please read more on :ref:`Usage`.

* Support for key-value storage which is backed by azure storage. Create an instance of `AzureBlobMap`

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
