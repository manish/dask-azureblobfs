.. _Usage:

=====
Usage
=====

To use Dask Azure Blob FileSystem in a project::

    from azureblobfs.dask import DaskAzureBlobFileSystem

This import makes sure that dask is aware of azure blob filesystem.
Next we import dask to read our data::

    import dask.dataframe as dd

Then you load your data as usual::

    data = dd.read_csv("abfs://account_name/mycontainer/weather*.csv",
        storage_options={"account_name": account_name,
            "account_key": account_key})

If you don't provide `account_name` or `account_key`, you would need
to set them via environment variables `AZURE_BLOB_ACCOUNT_NAME` and
`AZURE_BLOB_ACCOUNT_KEY` respectively. In which case your code would
be much simpler::

    data = dd.read_csv("abfs://account_name/mycontainer/weather*.csv")

The `account_name` in the URL is the same as `AZURE_BLOB_ACCOUNT_NAME`,
so you can remove a lot more of the hardcoding::

    data = dd.read_csv("abfs://{account_name}/mycontainer/weather*.csv"
        .format(account_name=os.environ.get("AZURE_BLOB_ACCOUNT_NAME")

You won't even have to hardcode `abfs://` if you want to use it from
`DaskAzureBlobFileSystem.protocol`. Now our code becomes more verbose,
but has even fewer hardcoding::

    data = dd.read_csv("{protocol}://{account_name}/mycontainer/weather*.csv"
        .format(protocol=DaskAzureBlobFileSystem.protocol,
            account_name=os.environ.get("AZURE_BLOB_ACCOUNT_NAME")

