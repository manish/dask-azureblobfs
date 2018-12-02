#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `azureblobfs` package."""

import os
import datetime
import unittest
import warnings
import time

import numpy

from azureblobfs.dask import DaskAzureBlobFileSystem
from azureblobfs.dask.mapping import AzureBlobMap
from azureblobfs.utils import generate_guid


class AzureBlobMapTest(unittest.TestCase):
    account_name = "e29"
    container = "azureblobmap"

    def setUp(self):
        self.account_name = self.account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        try:
            warnings.simplefilter("ignore", ResourceWarning)
        except:
            pass

        self.dask_fs = DaskAzureBlobFileSystem(self.account_name, self.account_key)
        self.azure_map = AzureBlobMap("{}/{}".format(self.container, generate_guid()), self.dask_fs)
        self.azure_map.clear()

    def test_AzureBlobMap_ctor(self):
        with self.assertRaises(TypeError):
            AzureBlobMap("magic", self.account_key)

    def test_get(self):
        self._populate_with_str_time_np()
        self.assertEqual(self.azure_map.get("first"), "First item")
        self.assertTrue(numpy.array_equal(self.azure_map.get("fourth"), numpy.array([1, 2, 3])))
        self.assertEqual(self.azure_map.get("unknown", "Not found"), "Not found")

    def test_clear(self):
        self._populate_with_str_time_np()
        self.assertEqual(len(self.azure_map), 4)
        self.azure_map.clear()
        self.assertEqual(len(self.azure_map), 0)

    def test_pop_present(self):
        self._populate_with_str_time_np()
        self.assertEqual(self.azure_map.pop("first"), "First item")
        self.assertTrue(numpy.array_equal(self.azure_map.pop("fourth"), numpy.array([1, 2, 3])))
        self.assertEqual(len(self.azure_map), 2)

    def test_pop_not_present_default(self):
        self.azure_map.clear()
        self.assertEqual(self.azure_map.pop("blah", "Nope"), "Nope")

    def test_pop_not_present_no_default(self):
        self.azure_map.clear()
        with self.assertRaises(KeyError):
            self.azure_map.pop("whatever")

    def test_keys(self):
        self._populate_with_str_time_np()
        all_keys = self.azure_map.keys()
        self.assertIn("first", all_keys)
        self.assertIn("second", all_keys)
        self.assertIn("third", all_keys)
        self.assertIn("fourth", all_keys)

    def test_values(self):
        self._populate_with_str_time_np()
        values = self.azure_map.values()
        self.assertIn("First item", values)
        self.assertIn("Second item", values)

    def test_items(self):
        self._populate_with_str_time_np()
        items = self.azure_map.items()
        self.assertIn(('first', 'First item'), items)
        self.assertIn(('second', 'Second item'), items)

    def test_del_present(self):
        self._populate_with_str_time_np()
        self.assertEqual(len(self.azure_map), 4)
        del self.azure_map["first"]
        self.assertEqual(len(self.azure_map), 3)
        del self.azure_map["third"]
        del self.azure_map["fourth"]
        self.assertEqual(len(self.azure_map), 1)

    def test_del_not_present(self):
        self._populate_with_str_time_np()
        with self.assertRaises(KeyError):
            del self.azure_map["some"]

    def test_setdefault_present(self):
        self._populate_with_str_time_np()
        self.assertEqual(self.azure_map.setdefault("first", "first not found"), "First item")

    def test_setdefault_not_present(self):
        self.assertEqual(self.azure_map.setdefault("abc", "something"), "something")
        self.assertEqual(len(self.azure_map), 1)
        self.assertEqual(self.azure_map["abc"], "something")

    def test_popitem_empty(self):
        with self.assertRaises(KeyError):
            self.azure_map.popitem()

    def test_popitem(self):
        self._populate_with_str_time_np()
        key, val = self.azure_map.popitem()
        self.assertEqual(key, "fourth")
        self.assertTrue(numpy.array_equal(val, numpy.array([1, 2, 3])))

    def _populate_with_str_time_np(self):
        self.azure_map["first"] = "First item"
        time.sleep(1)
        self.azure_map["second"] = "Second item"
        time.sleep(1)
        self.azure_map["third"] = datetime.datetime.utcnow()
        time.sleep(1)
        self.azure_map["fourth"] = numpy.array([1, 2, 3])
