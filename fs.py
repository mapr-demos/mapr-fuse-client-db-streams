#!/usr/bin/env python

from __future__ import with_statement

import os
import time
import sys
import errno
import json
import re
from collections import defaultdict

from fuse import FUSE, FuseOSError, Operations

fake_dir = re.compile(".*\\.dir$")

class Passthrough(Operations):
    end_offset = defaultdict(lambda: 1 << 48)
    stream_count = 0
    def decrement(self):
        self.stream_count = self.stream_count-1
        return self.stream_count

    def __init__(self, root):
        self.root = root
        self.open_streams = defaultdict(self.decrement)

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def is_fake_dir(self, full_path):
        return re.match(fake_dir, full_path)

    def is_fake_file(self, full_path):
        return self.is_fake_dir(os.path.dirname(full_path))

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        print("access %s\n", full_path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        print("chmod %s\n", full_path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        print("chown %s\n", full_path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        if self.is_fake_file(full_path):
            st = os.lstat(os.path.dirname(full_path))
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            r['st_size'] = self.end_offset[path]
        elif self.is_fake_dir(full_path):
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            r['st_mode'] = r['st_mode'] ^ 0o140000
        else:
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        return r

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        elif self.is_fake_dir(full_path):
            with open(full_path, 'r') as f:
                content = json.load(f)
            dirents.extend([d['name'] for d in content])
        else:
            print("mismatch: %s" % full_path)
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        print("opening from %s, %o" % (full_path, flags))
        if self.is_fake_file(full_path):
            return self.open_streams[full_path]
        else:
            return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        full_path = self._full_path(path)
        print("reading from %s,%d,%d,%s", (path,length,offset,str(fh)))
        if 
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        print("release %s, %d" % (path, fh))
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

def get_message(origin, offset):
    t = time.time() - origin
    if offset > t / 2:
        return None
    else:
        return "data t=%.1f" % (2*offset + origin)

def get_message_from_offset(stream, offset):
    if offset < 0:
        offset = 0
    messages = content[stream]
    old = 0
    c = 0
    i = 0
    while i < len(messages):
        dc = len(messages[i]['data']) + len(delimiter)
        if c + dc > offset:
            break
        i = i+1
        c = c + 

    # end of data
    if i >= len(messages):
        return (i, 0)

    # offset is in message i
    return (i, offset-c)

def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
