#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import os
import unittest
import azureblobfs

import dask.bytes.core

from azureblobfs.fs import AzureBlobFileSystem
from azureblobfs.utils import generate_guid
from azure.storage.blob.blockblobservice import BlockBlobService

class SplitContainerBlobTest(unittest.TestCase):
    account_name = "e29"
    container = "azure-blob-filesystem"

    def setUp(self):
        self.account_name = self.account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        self.connection = BlockBlobService(account_name=self.account_name, account_key=self.account_key)
        self.fs = AzureBlobFileSystem(self.container, self.connection)

    def test_ls(self):
        folder_list = self.fs.ls()
        self.assertIn("artifacts/", folder_list)
        self.assertIn("weathers/", folder_list)
        self.assertIn("03_section_zhen.pdf", folder_list)
        self.assertIn("image.png", folder_list)

    def test_ls_pattern(self):
        folder_list = self.fs.ls("artifacts*")
        self.assertIn("artifacts/samples/artifacts (2).json", folder_list)
        self.assertIn("artifacts/samples/artifacts (3).json", folder_list)
