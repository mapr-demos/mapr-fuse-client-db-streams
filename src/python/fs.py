#!/usr/bin/env python

from __future__ import with_statement

import errno
import json
import os
import re
import subprocess
import sys
from collections import namedtuple
from fuse import FUSE, FuseOSError, Operations

from mapr_streams_python import Consumer, KafkaError

fake_dir = re.compile(".*\\.dir$")
topic = re.compile(".*\\.dir\\*")


def get_stream(stream, topic_name):
    c = Consumer({'group.id': 'mygroup', 'default.topic.config': {'auto.offset.reset': 'earliest'},
                  'auto.commit.interval.ms': 500, 'message.max.bytes': 4096})
    c.subscribe([stream + ':' + topic_name])
    return c


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

    def is_fake_file(self, full_path):
        return self.is_fake_dir(os.path.dirname(full_path))

    @staticmethod
    def clean_string(raw_string):
        raw_string = re.sub('\\\\.', raw_string, '\\$')
        return re.sub(r"[^A-Za-z0-9{}:,\[\]\"_]+", "", raw_string)[1:]

    @staticmethod
    def transform_json_to_object(json_str):
        return json.loads(json_str, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

    def get_topics(self, full_path):
        topic_names = []

        topics_info = self.get_all_topics_info(full_path)

        if topics_info.status == 'ERROR':
            return topic_names

        for all_topic_info in topics_info.data:
            topic_names.append(all_topic_info.topic)

        return topic_names

    def get_all_topics_info(self, full_path):
        p = subprocess.Popen(['maprcli', 'stream', 'topic', 'list', '-path', full_path, '-json'],
                             stdout=subprocess.PIPE)
        json_str = self.clean_string(str(p.communicate()[0]))
        return self.transform_json_to_object(json_str)

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
        full_path = self._full_path(path)

        if self.is_fake_file(full_path):
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
            # p = subprocess.Popen(['maprcli', 'stream', 'topic', 'info', '-path', os.path.dirname(full_path),
            #                       '-topic', os.path.basename(full_path), '-json'], stdout=subprocess.PIPE)
            #
            # json_str = self.clean_string(str(p.communicate()[0]))
            # info_obj = self.transform_json_to_object(json_str)
            r['st_size'] = int(4068)
            # info_obj.data[0].physicalsize
        elif self.is_fake_dir(full_path):
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
        else:
            st = os.lstat(full_path)
            r = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                         'st_uid'))
        return r

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            print(os.listdir(full_path))
            dirents.extend(os.listdir(full_path))
        elif self.is_fake_dir(full_path):
            dirents.extend(self.get_topics(full_path))
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
        full_path = self._full_path(path)
        if self.is_fake_dir(full_path):
            subprocess.Popen(['maprcli', 'stream', 'delete', '-path', '/' + full_path], stdout=subprocess.PIPE)
            return os.remove(full_path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        full_path = self._full_path(path)
        if self.is_fake_file(full_path):
            subprocess.Popen(['maprcli', 'stream', 'topic', 'create', '-path', '/' + os.path.dirname(full_path),
                              '-topic', os.path.basename(full_path)], stdout=subprocess.PIPE)
            open(full_path, "w+")
            return None
        if self.is_fake_dir(full_path):
            subprocess.Popen(['maprcli', 'stream', 'create', '-path', '/' + full_path], stdout=subprocess.PIPE)
            return os.mkdir(self._full_path(path), mode)
        else:
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

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        print("create  %s" % path)
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        full_path = self._full_path(path)
        if self.is_fake_file(full_path):
            c = get_stream('/' + os.path.dirname(full_path), os.path.basename(full_path))

            data = b''
            try:
                for i in range(10):
                    msg = c.poll(timeout=2)
                    if msg is None:
                        return data
                    elif not msg.error():
                        data = data.__add__(msg.value())
            except Exception:
                return data

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
