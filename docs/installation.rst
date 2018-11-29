.. highlight:: shell

============
Installation
============


Stable release
--------------

To install Dask Azure Blob FileSystem, run this command in your terminal:

.. code-block:: console

    $ pip install dask-azureblobfs

This is the preferred method to install Dask Azure Blob FileSystem, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/

If you want to install a specific development version of `dask-azureblobfs`
using `pipenv`, run this command in your terminal

.. code-block:: console

    $ pipenv install -e git+https://github.com/manish/dask-azureblobfs#egg=dask-azureblobfs

This will install the package from github and update your `Pipfile` and `Pipfile.lock`

From sources
------------

The sources for Dask Azure Blob FileSystem can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/manish/dask-azureblobfs

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/manish/dask-azureblobfs/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/manish/dask-azureblobfs
.. _tarball: https://github.com/manish/dask-azureblobfs/tarball/master
