# -*- coding: utf-8 -*-
#
# The MIT License
#
# Copyright (c) 2018 Microsoft Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import pickle
from collections import MutableMapping

from azureblobfs.dask import DaskAzureBlobFileSystem

class AzureBlobMap(MutableMapping):
    def __init__(self, location, fs):
        if not isinstance(fs, DaskAzureBlobFileSystem):
            raise TypeError("Argument 'fs' needs to be of type azureblobfs.dask.DaskAzureBlobFileSystem")
        self.location = location
        self.fs = fs

    def clear(self):
        for matches_entry in self.fs.glob(self.location):
            container, blob = self.fs.split_container_blob(matches_entry)
            self.fs.rm(container, blob)

    def get(self, key, default_value=None):
        try:
            return self[key]
        except:
            return default_value

    def items(self):
        return list(map(lambda key: (key, self.__getitem__(key)), self.keys()))

    def keys(self):
        return list(self.__iter__())

    def pop(self, key, default_value=None):
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default_value is not None:
                return default_value
            raise

    def popitem(self):
        key = timestamp = None
        for matches_entry in self.fs.glob(self.location):
            container, blob = self.fs.split_container_blob(matches_entry)
            last_modified = self.fs.last_modified(container, blob)
            if timestamp is None or last_modified > timestamp:
                timestamp = last_modified
                key = matches_entry
        if key is None:
            raise KeyError()
        prefix = self.fs.join_container_blob(self.location, "")
        key = key.replace(prefix, "")
        return (key, self.pop(key))

    def setdefault(self, key, default_value=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default_value
            return default_value

    def values(self):
        return list(map(self.__getitem__, self.__iter__()))

    def __setitem__(self, key, value):
        container, blob_prefix = DaskAzureBlobFileSystem.split_container_blob(self.location)
        blob_path = DaskAzureBlobFileSystem.join_container_blob(blob_prefix, key)
        self.fs.connection.create_blob_from_bytes(container, blob_path, pickle.dumps(value))

    def __delitem__(self, key):
        container, blob_prefix = DaskAzureBlobFileSystem.split_container_blob(self.location)
        blob_path = DaskAzureBlobFileSystem.join_container_blob(blob_prefix, key)
        if not self.fs.exists(container, blob_path):
            raise KeyError (key)
        self.fs.rm(container, blob_path)

    def __getitem__(self, key):
        full_path = self.fs.join_container_blob(self.location, key)
        container, blob_path = DaskAzureBlobFileSystem.split_container_blob(full_path)
        if not self.fs.exists(container, blob_path):
            raise KeyError (key)
        with self.fs.open(full_path) as f:
            return pickle.loads(f.read())

    def __iter__(self):
        prefix = self.fs.join_container_blob(self.location, "")
        return map(lambda x: x.replace(prefix, ""), self.fs.glob(self.location))

    def __len__(self):
        return len([_ for _ in self.fs.glob(self.location)])
