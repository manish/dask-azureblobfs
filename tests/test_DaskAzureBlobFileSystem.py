#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import os
import unittest
import warnings
import urllib.request

import dask.dataframe as dd

from azureblobfs.dask import DaskAzureBlobFileSystem

class DaskAzureBlobFileSystemTest(unittest.TestCase):
    account_name = "e29"
    container = "public"
    blob_pattern = "split_*.csv"

    def setUp(self):
        self.account_name = self.account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        warnings.simplefilter("ignore", ResourceWarning)
        self.url = "{protocol}://{path}".format(protocol=DaskAzureBlobFileSystem.protocol,
            path=DaskAzureBlobFileSystem.sep.join([self.account_name, self.container, self.blob_pattern]))
        self.storage_options = {"account_name": self.account_name, "account_key": self.account_key}

    def test_partitions_shape(self):
        split_data = dd.read_csv(self.url, storage_options=self.storage_options)
        self.assertEqual(split_data.npartitions, 10)

        split_rows, split_columns = split_data.shape
        self.assertEqual(split_rows.compute(), 337040)

    def test_compute(self):
        local_file = os.path.join(os.getcwd(), "tests/testdata/test_first_337040.csv")
        if not os.path.exists(os.path.dirname(local_file)):
            os.mkdir(os.path.dirname(local_file))
        urllib.request.urlretrieve("https://e29.blob.core.windows.net/public/test_first_337040.csv", local_file)

        unified_data = dd.read_csv(local_file)
        split_data = dd.read_csv(self.url, storage_options=self.storage_options)
        self.assertEqual(unified_data.max().compute().id, split_data.max().compute().id)
        self.assertEqual(unified_data.min().compute().id, split_data.min().compute().id)
        self.assertEqual(unified_data.mean().compute().id, split_data.mean().compute().id)
