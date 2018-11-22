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

import io
import os
import fnmatch

from azure.storage.blob.blockblobservice import BlockBlobService

class AzureBlobFile(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def read(self, length):
        pass

    def tell(self):
        pass

    def close(self):
        pass

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

class AzureBlobFileSystem(object):
    def __init__(self, container, service, **kwargs):
        if not isinstance(service, BlockBlobService):
            raise TypeError("service needs to be of type azure.storage.blob.blockblobservice.BlockBlobService")
        self.service = service
        self.kwargs = kwargs
        self.container = container
        self.cwd = ""
        self.sep = "/"

    def ls(self, pattern=None):
        subpath = self._ls_subfolder(self.service.list_blobs(self.container))
        if not pattern:
            return set(map(lambda x: x[:x.find("/")+1] if x.find("/") >=0 else x, subpath))
        else:
            return set(filter(lambda x: fnmatch.fnmatch(x, pattern), subpath))

    def mkdir(self, dir_name):
        pass

    def cd(self, dir_name=None):
        if dir_name == None:
            self.cwd = ""
        elif "{dir_name}{sep}".format(dir_name=dir_name, sep=self.sep) in self.ls():
            self.cwd = dir_name if self.cwd == "" else "{cwd}{sep}{dir_name}".format(cwd = self.cwd, sep=self.sep, dir_name=dir_name)
        else:
            raise IOError("Directory '{dir_name}' does not exist under '{cwd}{sep}'".format(dir_name=dir_name, cwd=self.cwd, sep=self.sep))

    def rm(self, file_name):
        full_path = self._create_full_path(file_name)
        if self.service.exists(self.container, full_path):
            path_delete_lease = None
            try:
                path_delete_lease = self.service.acquire_blob_lease(self.container, full_path)
                self.service.delete_blob(self.container, full_path, lease_id=path_delete_lease)
            except:
                if path_delete_lease is not None:
                    self.service.release_blob_lease(self.container, full_path, path_delete_lease)
        else:
            raise IOError(
                "File '{file}' does not exist under '{cwd}{sep}'".format(file=file, cwd=self.cwd, sep=self.sep))

    def touch(self, file):
        full_path = self._create_full_path(file)
        container_lease = None
        try:
            container_lease = self.service.acquire_container_lease(self.container)
            self.service.create_blob_from_text(self.container, full_path, "")
        finally:
            if container_lease is not None:
                self.service.release_container_lease(self.container, container_lease)
        return full_path

    def rmdir(self, path):
        pass

    def mv(self, src_path, dst_path):
        pass

    def cp(self, src_path, dst_path):
        pass

    def pwd(self):
        return self.cwd

    def du(self):
        return { blob.name : blob.properties.content_length
                 for blob in self.service.list_blobs(self.container) }

    def head(self, path, bytes_count):
        return self.service.get_blob_to_bytes(self.container, self._create_full_path(path), start_range=0,
                                              end_range=bytes_count-1).content

    def tail(self, path, bytes_count):
        full_path = self._create_full_path(path)
        size = self.service.get_blob_properties(self.container, full_path).properties.content_length
        return self.service.get_blob_to_bytes(self.container, full_path, start_range=size-bytes_count,
                                              end_range=size-1).content

    def _ls_subfolder(self, blobs):
        subpath = map(lambda blob: blob.replace(self.cwd, ""), [item.name for item in blobs])
        return map(lambda blob: blob[1:] if blob.startswith(self.sep) else blob, subpath)

    def _create_full_path(self, file_name):
        return file_name if self.cwd == "" else "{cwd}{sep}{path}".format(cwd=self.cwd, sep=self.sep, path=file_name)

class AzureBlobMap(object):
    def __init__(self, location, fs):
        self.location = location
        self.fs = fs

    def clear(self):
        pass

    def get(self, key, default_value=None):
        pass

    def items(self):
        pass

    def keys(self):
        pass

    def pop(self, key, default_value=None):
        pass

    def popitem(self):
        pass

    def setdefault(self, key, default_value=None):
        pass

    def update(self, key, **value):
        pass

    def values(self):
        pass
