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

    def test_cd_pwd(self):
        self.fs.cd()
        self.assertEqual(self.fs.pwd(), "")
        self.fs.cd("artifacts")
        self.assertEqual(self.fs.pwd(), "artifacts")
        self.fs.cd()
        self.assertEqual(self.fs.pwd(), "")
        self.fs.cd("artifacts")
        self.fs.cd("samples")
        self.assertEqual(self.fs.pwd(), "artifacts/samples")

    def test_cd_fails(self):
        with self.assertRaises(IOError) as context:
            self.fs.cd()
            self.assertEqual(self.fs.pwd(), "")
            self.fs.cd("undefined_folder")

    def test_touch_rm(self):
        self.fs.cd()
        file_name = "test_touch_rm/{file_name}.txt".format(file_name=generate_guid())
        with self.assertRaises(IOError) as context:
            self.fs.rm(file_name)
        self.assertEqual(self.fs.touch(file_name), file_name)
        self.fs.rm(file_name)

    def test_du(self):
        self.fs.cd()
        du_results = self.fs.du()
        self.assertIn(('weathers/rdu-weather-history.csv', 480078), du_results.items())
        self.assertIn(('weathers/Local_Weather_Data.csv', 7580289), du_results.items())

    def test_head(self):
        self.fs.cd()
        head_content = self.fs.head("weathers/Local_Weather_Data.csv", 20)
        self.assertEqual(len(head_content), 20)
        self.assertEqual(head_content, b'RowID,DateTime,TempO')

    def test_tail(self):
        self.fs.cd()
        tail_content = self.fs.tail("weathers/Local_Weather_Data.csv", 50)
        self.assertEqual(len(tail_content), 50)
        self.assertEqual(tail_content, b'.201,0,68.7,38,42.1,66.1,7.55,0.0748,351,1,100,15\n')

