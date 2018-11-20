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

ab_protocol = "abfs"

class DaskAzureBlobFileSystem(LocalFileSystem):
    def __init__(self, account_name=None, account_key=None, sas_token=None, **storage_options):
        super(LocalFileSystem, self).__init__(storage_options)

        account_name = account_name or os.environ.get("AZURE_BLOB_ACCOUNT_NAME")
        account_key = account_key or os.environ.get("AZURE_BLOB_ACCOUNT_KEY")
        sas_token = sas_token or os.environ.get("AZURE_BLOB_SAS_TOKEN")
        self.connection = BlockBlobService(account_name=account_name,
                                               account_key=account_key,
                                               sas_token=sas_token,
                                               protocol=storage_options.get("protocol"),
                                               endpoint_suffix=storage_options.get("endpoint_suffix"),
                                               custom_domain=storage_options.get("custom_domain"))

    def glob(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return filter(lambda x: fnmatch.fnmatch(x.name, blob_pattern), self.connection.list_blobs(container))

    def mkdirs(self, path, **kwargs):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        if not self.connection.exists(container):
            self.connection.create_container(container, kwargs)

    def open(self, path, mode='rb', **kwargs):
        pass

    def size(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return self.connection.get_blob_properties(container, blob_pattern).properties.content_length

    def ukey(self, path):
        container, blob_pattern = DaskAzureBlobFileSystem.split_container_blob(path)
        return self.connection.get_blob_properties(container, blob_pattern).properties.etag

    @classmethod
    def split_container_blob(self, path):
        index_sep = path.find("/")
        if index_sep < 0:
            raise Exception("The path provided is not in the format {protocol}://container/blob_pattern".format(protocol=ab_protocol))
        return path[:index_sep], path[index_sep+1:]

dask.bytes.core._filesystems[ab_protocol] = DaskAzureBlobFileSystem
