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

import os
import fnmatch

from azure.storage.blob.blockblobservice import BlockBlobService

import dask.bytes.core
from dask.bytes.local import LocalFileSystem

from azureblobfs.fs import AzureBlobReadableFile, AzureBlobFileSystem

class DaskAzureBlobFileSystem(AzureBlobFileSystem, LocalFileSystem):
    protocol="abfs"
    sep = "/"
    def __init__(self, account_name=None, account_key=None, sas_token=None, connection_string=None, **storage_options):
        super(DaskAzureBlobFileSystem, self).__init__(account_name=account_name, account_key=account_key, sas_token=sas_token, connection_string=connection_string, storage_options=storage_options)

    def glob(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return map(lambda x: DaskAzureBlobFileSystem.join_container_blob(container, x.name),
                   filter(lambda x: fnmatch.fnmatch(x.name, blob_pattern), self.connection.list_blobs(container)))

    def mkdirs(self, path, **kwargs):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        if not self.connection.exists(container):
            self.connection.create_container(container, kwargs)

    def open(self, path, mode='rb', **kwargs):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return AzureBlobReadableFile(self.connection, container, blob_pattern, mode)

    def size(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return self.connection.get_blob_properties(container, blob_pattern).properties.content_length

    def ukey(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return self.connection.get_blob_properties(container, blob_pattern).properties.etag

    @classmethod
    def split_container_blob(self, path):
        trimmed_path = path[1:] if path.startswith(self.sep) else path
        index_sep = trimmed_path.find(self.sep)
        if index_sep <= 0:
            raise ValueError("The path provided is not in the correct\n Expected format: '{protocol}://account/container/blob_pattern'\nFound: '{path}'".format(
                protocol=DaskAzureBlobFileSystem.protocol, path=path))
        return trimmed_path[:index_sep], trimmed_path[index_sep+1:]

    @classmethod
    def join_container_blob(self, container, blob):
        return "{container}{sep}{blob}".format(container=container, sep=DaskAzureBlobFileSystem.sep, blob=blob)

dask.bytes.core._filesystems[DaskAzureBlobFileSystem.protocol] = DaskAzureBlobFileSystem
