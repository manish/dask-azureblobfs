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
        self.tmp_dir = tempfile.mkdtemp(generate_guid())
        self.tmp_path = os.path.join(self.tmp_dir, self.blob_path.replace("/", "-"))
        self.connection.get_blob_to_path(self.container, self.blob_path, tmp_path)
        self.fid = open(self.tmp_path, "rb" if not self.is_content_type_text else 'r')
        self.fid.seek(0)
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

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
        self.touch("{dir_name}/".format(dir_name=dir_name))

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
                "File '{file}' does not exist under '{cwd}{sep}'".format(file=file_name, cwd=self.cwd, sep=self.sep))

    def touch(self, file_name):
        full_path = self._create_full_path(file_name)
        container_lease = None
        try:
            container_lease = self.service.acquire_container_lease(self.container)
            self.service.create_blob_from_text(self.container, full_path, "")
        finally:
            if container_lease is not None:
                self.service.release_container_lease(self.container, container_lease)
        return full_path

    def mv(self, src_path, dst_path):
        try:
            self.cp(src_path, dst_path)
            self.rm(src_path)
            return True
        except:
            self.rm(dst_path)
            return False

    def cp(self, src_path, dst_path):
        copy_container_lease = None
        full_src_path = self._create_full_path(src_path)
        full_dst_path = self._create_full_path(dst_path)
        try:
            copy_container_lease = self.service.acquire_container_lease(self.container)
            self.service.copy_blob(self.container, full_dst_path, self.service.make_blob_url(self.container, full_src_path))
        finally:
            if copy_container_lease is not None:
                self.service.release_container_lease(self.container, copy_container_lease)

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
