#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import unittest
import azureblobfs

import dask.bytes.core

from azureblobfs import DaskAzureBlobFileSystem

class SplitContainerBlobTest(unittest.TestCase):

    def test_simple_scenario_success(self):
        container, blob = azureblobfs.DaskAzureBlobFileSystem.split_container_blob("azureblobfs/blob_name")
        self.assertEqual(container, "azureblobfs")
        self.assertEqual(blob, "blob_name")

    def test_subfolder_scenario_success(self):
        container, blob = azureblobfs.DaskAzureBlobFileSystem.split_container_blob("azureblobfs/subfolder/blob_name")
        self.assertEqual(container, "azureblobfs")
        self.assertEqual(blob, "subfolder/blob_name")

    def test_no_blob_failure(self):
        with self.assertRaises(Exception) as context:
            azureblobfs.DaskAzureBlobFileSystem.split_container_blob("azureblobfs")

    def test_is_registered(self):
        self.assertIn(azureblobfs.core.ab_protocol, dask.bytes.core._filesystems)

class DaskAzureBlobFileSystemTest(unittest.TestCase):

    account_name="e29"
    container_name = "abfs-methods"
    files = { "Local_Weather_Data.csv": {"size": 7580289, "ukey": "\"0x8D64E894851407E\""},
              "rdu-weather-history.csv": {"size": 480078, "ukey": "\"0x8D64E894B716BBD\""}}
    def setUp(self):
        self.fs = DaskAzureBlobFileSystem(account_name=self.account_name)

    def test_glob(self):
        all_files = [file.name for file in self.fs.glob("{container_name}/*.csv".format(container_name=self.container_name))]
        for file in self.files:
            self.assertIn(file, all_files)

    def test_ukey(self):
        for file, props in self.files.items():
            found_ukey = self.fs.ukey("{container}/{blob}".format(container=self.container_name, blob=file))
            expected_ukey =  props["ukey"]
            self.assertEqual(found_ukey, expected_ukey,
                             "For blob '{blob}', expected ukey to be '{expected_ukey}' but found '{found_ukey}'".format(
                                 blob=file, expected_ukey=expected_ukey, found_ukey=found_ukey))

    def test_size(self):
        for file, props in self.files.items():
            found_size = self.fs.size("{container}/{blob}".format(container=self.container_name, blob=file))
            expected_size =  props["size"]
            self.assertEqual(found_size, expected_size,
                             "For blob '{blob}', expected size to be '{expected_size}' but found '{found_size}'".format(
                                 blob=file, expected_size=expected_size, found_size=found_size))
