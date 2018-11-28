#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import os
import tempfile
import unittest
import warnings
import urllib.request

import numpy

from azureblobfs.fs import AzureBlobReadableFile
from azure.storage.blob.blockblobservice import BlockBlobService


class AzureBlobReadableTextFileTest(unittest.TestCase):
    account_name = "e29"
    container = "azure-blob-filesystem"
    text_blob_name = "weathers/Local_Weather_Data.csv"
    binary_blob_name = "weathers/abc.nyc"

    def setUp(self):
        self.account_name = self.account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        warnings.simplefilter("ignore", ResourceWarning)

        self.connection = BlockBlobService(account_name=self.account_name, account_key=self.account_key)

    def test_seek_tell(self):
        with AzureBlobReadableFile(self.connection, self.container, self.text_blob_name) as fid:
            self.assertEqual(fid.tell(), 0)
            self.assertEqual(fid.seek(30), 30)
            self.assertEqual(fid.seek(30), 30)

    def test_text_read_seek(self):
        with AzureBlobReadableFile(self.connection, self.container, self.text_blob_name, mode='r') as fid:
            self.assertEqual(fid.read(50), b'RowID,DateTime,TempOut,HiTemp,LowTemp,OutHum,DewPt')
            self.assertEqual(fid.read(30), b',WindSpeed,WindDir,WindRun,HiS')
            self.assertEqual(fid.seek(100), 100)
            self.assertEqual(fid.read(30), b',HeatIndex,THWIndex,Bar,Rain,R')
            self.assertEqual(fid.seek(50), 50)
            self.assertEqual(fid.read(30), b',WindSpeed,WindDir,WindRun,HiS')

    def test_binary_read_seek(self):
        expected_np_array = numpy.array([2, 34, 5, 3, 4])
        with AzureBlobReadableFile(self.connection, self.container, self.binary_blob_name) as fid:
            self.assertEqual(fid.tell(), 0)
            for size in [None, 248, 5000]:
                with self.subTest(size=size):
                    self.assertEqual(fid.seek(0), 0)
                    with tempfile.NamedTemporaryFile() as tmp_f:
                        tmp_f.write(fid.read(size))
                        tmp_f.seek(0)
                        found_np_array = numpy.load(tmp_f)
                        self.assertTrue(
                            numpy.array_equal(found_np_array, expected_np_array),
                            "Expected numpy array to be {expected} but found {found}".format(
                                expected=expected_np_array, found=found_np_array))

    def test_readline(self):
        local_file = os.path.join(os.getcwd(), "tests/testdata/Local_Weather_data.csv")
        if not os.path.exists(os.path.dirname(local_file)):
            os.mkdir(os.path.dirname(local_file))
        urllib.request.urlretrieve("https://e29.blob.core.windows.net/public/Local_Weather_Data.csv", local_file)

        with AzureBlobReadableFile(self.connection, self.container, self.text_blob_name, mode='r') as remote_fid,\
                open(local_file) as local_fid:
            for expected_line in local_fid:
                actual_line = remote_fid.readline()
                self.assertEqual(actual_line.decode("utf-8"), expected_line,
                                 "\nError:\nActual:\n{actual_line}\nExpected:\n{expected_line}".format(
                                     actual_line=actual_line, expected_line=expected_line))
