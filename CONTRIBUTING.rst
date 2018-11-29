.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/manish/dask-azureblobfs/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

Dask Azure Blob FileSystem could always use more documentation, whether as part of the
official Dask Azure Blob FileSystem docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/manish/dask-azureblobfs/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `dask-azureblobfs` for local development.

1. Fork the `dask-azureblobfs` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/dask-azureblobfs.git

3. Install docker as we use `docker` and `pipenv` for creating a repeatable environment, this is how you set up your fork for local development::

    $ cd dask-azureblobfs/
    $ docker-compose build

If you get an error which looks like::

    WARNING: The AZURE_BLOB_ACCOUNT_NAME variable is not set. Defaulting to a blank string.
    WARNING: The AZURE_BLOB_ACCOUNT_KEY variable is not set. Defaulting to a blank string.

then you will need to create an Azure account, create an instance of `Azure Blob Storage` and set the environment variables
with the respective azure blob storage account name and the key associated with that account.

If you are not comfortable using the key, you can generate a shared access signature and set `AZURE_BLOB_SAS_TOKEN`.

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8, the
tests pass and the docs can be generated successfully.::

    $ ./drun_app make lint
    $ ./drun_app make test
    $ ./drun_app make docs

You don't need to install any package as `docker-compose build` command takes
care of installing all the required packages and creating the container.

6. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. Check https://travis-ci.org/manish/dask-azureblobfs/pull_requests
   and make sure that the tests pass for all supported Python versions.

Tips
----

To run a subset of tests::

    $ ./drun_app pytest tests/test_AzureBlobMap.py

which will run all the test fixtures in that file::

    $ ./drun_app pytest tests/test_AzureBlobMap.py::AzureBlobMapTest

will run only the specific fixture::

    $ ./drun_app pytest tests/test_AzureBlobMap.py::AzureBlobMapTest::test_keys

will run only the specific test


Deploying
---------

A reminder for the maintainers on how to deploy.
Make sure all your changes are committed (including an entry in HISTORY.rst).
Then run::

$ bumpversion patch # possible: major / minor / patch
$ git push
$ git push --tags

Travis will then deploy to PyPI if tests pass.
