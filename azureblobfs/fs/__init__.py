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
    def __init__(self, service, container, **kwargs):
        self.service = service
        self.kwargs = kwargs
        self.container = container
        self.cwd = ""

    def ls(self, pattern):
        pass

    def mkdir(self, dir_name):
        pass

    def cd(self, dir_name=None):
        pass

    def rm(self, path):
        pass

    def rmdir(self, path):
        pass

    def mv(self, src_path, dst_path):
        pass

    def cp(self, src_path, dst_path):
        pass

    def pwd(self):
        pass

    def df(self):
        pass

    def du(self):
        pass

    def head(self, bytes_count=None):
        pass

    def tail(self, bytes_count=None):
        pass

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
