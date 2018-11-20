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
    def test_glob(self):
        fs = DaskAzureBlobFileSystem(account_name=self.account_name)
        all_files = [file.name for file in fs.glob("{container_name}/*.csv".format(container_name=self.container_name))]
        self.assertIn("Local_Weather_Data.csv", all_files)
        self.assertIn("rdu-weather-history.csv", all_files)
