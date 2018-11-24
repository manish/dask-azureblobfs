#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import os
import unittest
import warnings

from azureblobfs.fs import AzureBlobReadableFile
from azure.storage.blob.blockblobservice import BlockBlobService

class AzureBlobReadableTextFileTest(unittest.TestCase):
    account_name = "e29"
    container = "azure-blob-filesystem"

    def setUp(self):
        self.account_name = self.account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        warnings.simplefilter("ignore", ResourceWarning)

        self.connection = BlockBlobService(account_name=self.account_name, account_key=self.account_key)
        self.fid = AzureBlobReadableFile(self.connection, self.container, "weathers/Local_Weather_Data.csv")

    def test_seek_whence_invalid(self):
        with self.assertRaises(ValueError) as context:
            self.fid.seek(0, 4)
        with self.assertRaises(ValueError) as context:
            self.fid.seek(0, -1)
