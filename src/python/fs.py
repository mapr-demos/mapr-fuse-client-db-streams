#!/usr/bin/env python

from __future__ import with_statement

import errno
import subprocess
import traceback
import time
import os
import re
import sys
from functools import lru_cache

from fuse import FUSE, FuseOSError, Operations

from stream import Stream

fake_dir = re.compile(".*\\.dir$")


@lru_cache(maxsize=32)
def get_stream(path):
    return Stream(path)


class Passthrough(Operations):

    def __init__(self, root):
        self.root = root

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    @staticmethod
    def is_fake_dir(full_path):
        return re.match(fake_dir, full_path)

    # @staticmethod
    # def is_stream(full_path):
    #     p = subprocess.Popen(['maprcli', 'stream', 'info', '-path', full_path], stdout=subprocess.PIPE)
    #     l = p.communicate()
    #     l[0].decode('utf-8')
    #     if 'ERROR (22)' in l[0].decode('utf-8'):
    #         return False
    #     return True

    def is_fake_file(self, full_path):
        return self.is_fake_dir(os.path.dirname(full_path))

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        # print("access %s\n" % full_path)
        if self.is_fake_file(full_path):
            print("fake file access")
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        # print("chmod %s\n", full_path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        # print("chown %s\n", full_path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):

        print("PARAMETERS (method getattr): ")
        print(path)
        print(fh)

        full_path = self._full_path(path)

        print("Full path: " + full_path)

        if self.is_fake_file(full_path):
            parent = os.path.dirname(full_path)
            st = os.lstat(parent)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
            r['st_size'] = get_stream(parent).size(os.path.basename(full_path))
            r['st_mtime'] = int(time.time())
        elif self.is_fake_dir(full_path):
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
            r['st_mode'] = r['st_mode'] ^ 0o140000
            r['st_mtime'] = int(time.time())

            print(r)
        else:
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
        return r

    def readdir(self, path, fh):

        print("PARAMETERS (method readdir): ")
        print(path)
        print(fh)

        full_path = self._full_path(path)

        print("Full path: " + full_path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        elif self.is_fake_dir(full_path):
            print("maprcli stream topic list -path -> " + full_path)
            p = subprocess.Popen(['maprcli', 'stream', 'topic', 'list', '-path', '/' + full_path],
                                 stdout=subprocess.PIPE)
            topics = p.communicate()[0].decode("utf-8").split('\n')
            type(topics)
            print(topics)
            dirents.extend(topics)
        else:
            print("mismatch: %s" % full_path)
        for r in dirents:
            yield r

    def readlink(self, path):
        # print("readlink %s" % path)
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        # print("rmdir %s" % path)
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        # print("mkdir %s" % path)
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        # print("statfs %s" % path)
        full_path = self._full_path(path)
        if self.is_fake_file(full_path):
            stv = os.statvfs(os.path.dirname(full_path))
        else:
            stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        # print("statfs %s" % path)
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        # print("symlink %s" % path)
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        # print("rename %s" % path)
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        # print("link %s" % path)
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        if self.is_fake_file(path):
            return os.utime(os.path.dirname(self._full_path(path)), times)
        else:
            print("utimens  %s" % path)
            return os.utime(self._full_path(path), times)

    # File methods
    # ============
    stream_count = 1000
    open_streams = dict()

    def open_stream(self, path):
        try:
            fd = self.open_streams[path]
        except KeyError:
            self.stream_count = self.stream_count + 1
            self.open_streams[path] = self.stream_count
            fd = self.stream_count
        return fd

    def open(self, path, flags):
        full_path = self._full_path(path)
        print("opening from %s, %o" % (full_path, flags))
        if self.is_fake_file(full_path):
            return self.open_stream(full_path)
        else:
            return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        print("create  %s" % path)
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        full_path = self._full_path(path)
        if self.is_fake_file(full_path):
            stream = os.path.dirname(full_path)
            topic = os.path.basename(full_path)
            try:
                data = get_stream(stream).read_bytes(topic, offset, length)
            except Exception as e:
                print('-' * 60)
                traceback.print_exc(file=sys.stdout)
                print('-' * 60)
                data = b''
            return data
        else:
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
        if fh > 1000:
            full_path = self._full_path(path)
            del self.open_streams[full_path]
        else:
            return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
