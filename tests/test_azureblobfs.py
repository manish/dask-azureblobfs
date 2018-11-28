#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import unittest
import warnings

import dask.bytes.core

from azureblobfs.dask import DaskAzureBlobFileSystem
from azureblobfs.utils import generate_guid


class SplitContainerBlobTest(unittest.TestCase):

    def setUp(self):
        self.container = generate_guid()
        self.subfolder = generate_guid(5)
        self.blob_name = generate_guid(10)
        warnings.simplefilter("ignore", ResourceWarning)

    def test_simple_scenario_success(self):
        container, blob = DaskAzureBlobFileSystem.split_container_blob("{container}/{blob_name}".format(
            container=self.container, blob_name=self.blob_name))
        self.assertEqual(container, self.container)
        self.assertEqual(blob, self.blob_name)

    def test_subfolder_scenario_success(self):
        container, blob = DaskAzureBlobFileSystem.split_container_blob("{container}/{subfolder}/{blob_name}".format(
            container=self.container, subfolder=self.subfolder, blob_name=self.blob_name))
        self.assertEqual(container, self.container)
        self.assertEqual(blob, "{subfolder}/{blob_name}".format(subfolder=self.subfolder, blob_name=self.blob_name))

    def test_no_blob_failure(self):
        with self.assertRaises(Exception):
            DaskAzureBlobFileSystem.split_container_blob(self.container)

    def test_is_registered(self):
        self.assertIn(DaskAzureBlobFileSystem.protocol, dask.bytes.core._filesystems)


class DaskAzureBlobFileSystemTest(unittest.TestCase):
    account_name = "e29"
    container_name = "abfs-methods"
    files = {"abfs-methods/Local_Weather_Data.csv": {"size": 7580289, "ukey": "\"0x8D64E894851407E\""},
             "abfs-methods/rdu-weather-history.csv": {"size": 480078, "ukey": "\"0x8D64E894B716BBD\""}}

    def setUp(self):
        self.fs = DaskAzureBlobFileSystem(account_name=self.account_name)
        warnings.simplefilter("ignore", ResourceWarning)

    def test_glob(self):
        all_files = [file for file in self.fs.glob("{container_name}/*.csv".format(container_name=self.container_name))]
        for file in self.files:
            self.assertIn(file, all_files)

    def test_ukey(self):
        for file, props in self.files.items():
            found_ukey = self.fs.ukey(file)
            expected_ukey = props["ukey"]
            self.assertEqual(found_ukey, expected_ukey,
                             "For blob '{blob}', expected ukey to be '{expected_ukey}' but found '{found_ukey}'".format(
                                 blob=file, expected_ukey=expected_ukey, found_ukey=found_ukey))

    def test_size(self):
        for file, props in self.files.items():
            found_size = self.fs.size(file)
            expected_size = props["size"]
            self.assertEqual(found_size, expected_size,
                             "For blob '{blob}', expected size to be '{expected_size}' but found '{found_size}'".format(
                                 blob=file, expected_size=expected_size, found_size=found_size))
