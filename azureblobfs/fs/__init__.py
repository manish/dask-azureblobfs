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
import tempfile
import fnmatch

from azure.storage.blob.blockblobservice import BlockBlobService

from azureblobfs.utils import generate_guid

class AzureBlobReadableFile(object):
    allowed_whence_values = (0, 1, 2)
    def __init__(self, connection, container, blob_path, mode='rb', **kwargs):
        self.connection = connection
        self.container = container
        self.blob_path = blob_path
        self.mode = mode
        self.kwargs = kwargs

        self.blob_props = self.connection.get_blob_properties(container, blob_path).properties
        self.size = self.blob_props.content_length
        self.content_type = self.blob_props.content_settings.content_type
        self.is_content_type_text = "text" in self.content_type or 'b' not in self.mode

        self.tmp_path = None
        self.fid = None
        self._open()

    def read(self, length=None):
        return self.fid.read(length)

    def readline(self):
        return self.fid.readline()

    def readlines(self):
        return self.fid.readlines()

    def seek(self, loc, whence=0):
        return self.fid.seek(loc, whence)

    def close(self):
        if self.fid is not None:
            self.fid.close()
            os.remove(self.tmp_path)
            self.fid = None
            self.tmp_path = None

    def tell(self):
        return self.fid.tell()

    def readable(self):
        return self.fid.readable()

    def seekable(self):
        return self.fid.seekable()

    def writable(self):
        return False

    def __enter__(self):
        if self.fid is None:
            self._open()
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def _open(self):
        self.tmp_dir = tempfile.mkdtemp(generate_guid())
        self.tmp_path = os.path.join(self.tmp_dir, self.blob_path.replace("/", "-"))
        self.connection.get_blob_to_path(self.container, self.blob_path, self.tmp_path)
        self.fid = open(self.tmp_path, "rb")
        self.fid.seek(0)

class AzureBlobFileSystem(object):
    def __init__(self, container, connection, **kwargs):
        if not isinstance(connection, BlockBlobService):
            raise TypeError("Argument connection needs to be of type azure.storage.blob.blockblobservice.BlockBlobService")
        self.connection = connection
        self.kwargs = kwargs
        self.container = container
        self.sep = "/"

    def ls(self, pattern=None):
        return list(set(filter(lambda x: fnmatch.fnmatch(x, pattern) if pattern else x,
                               map(lambda x: x.name, self.connection.list_blobs(self.container)))))

    def mkdir(self, dir_name):
        self.touch("{dir_name}/".format(dir_name=dir_name))

    def rm(self, full_path):
        if self.connection.exists(self.container, full_path):
            path_delete_lease = None
            try:
                path_delete_lease = self.connection.acquire_blob_lease(self.container, full_path)
                self.connection.delete_blob(self.container, full_path, lease_id=path_delete_lease)
            except:
                if path_delete_lease is not None:
                    self.connection.release_blob_lease(self.container, full_path, path_delete_lease)
        else:
            raise IOError(
                "File '{file}' does not exist under container '{container}'".format(file=full_path, container=self.container))

    def touch(self, full_path):
        container_lease = None
        try:
            container_lease = self.connection.acquire_container_lease(self.container)
            self.connection.create_blob_from_text(self.container, full_path, "")
        finally:
            if container_lease is not None:
                self.connection.release_container_lease(self.container, container_lease)
        return full_path

    def mv(self, src_path, dst_path):
        try:
            self.cp(src_path, dst_path)
            self.rm(src_path)
            return True
        except:
            self.rm(dst_path)
            return False

    def cp(self, full_src_path, full_dst_path):
        copy_container_lease = None
        try:
            copy_container_lease = self.connection.acquire_container_lease(self.container)
            self.connection.copy_blob(self.container, full_dst_path, self.connection.make_blob_url(self.container, full_src_path))
        finally:
            if copy_container_lease is not None:
                self.connection.release_container_lease(self.container, copy_container_lease)

    def du(self):
        return { blob.name : blob.properties.content_length
                 for blob in self.connection.list_blobs(self.container) }

    def head(self, full_path, bytes_count):
        return self.connection.get_blob_to_bytes(self.container, full_path, start_range=0,
                                              end_range=bytes_count-1).content

    def tail(self, full_path, bytes_count):
        size = self.connection.get_blob_properties(self.container, full_path).properties.content_length
        return self.connection.get_blob_to_bytes(self.container, full_path, start_range=size-bytes_count,
                                              end_range=size-1).content
