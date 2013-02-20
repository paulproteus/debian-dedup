#!/usr/bin/python
"""
CREATE TABLE content (package TEXT, version TEXT, architecture TEXT, filename TEXT, size INTEGER, hash TEXT);
CREATE INDEX content_package_index ON content (package);
CREATE INDEX content_hash_index ON content (hash);
"""

import hashlib
import os
import re
import sqlite3
import struct
import sys
import tarfile

import apt_pkg
import lzma

apt_pkg.init()

class ArReader(object):
    global_magic = b"!<arch>\n"
    file_magic = b"`\n"

    def __init__(self, fileobj, membertest):
        self.fileobj = fileobj
        self.membertest = membertest
        self.remaining = None

    def skip(self, length):
        while length:
            data = self.fileobj.read(min(4096, length))
            if not data:
                raise ValueError("archive truncated")
            length -= len(data)

    def skiptillmember(self):
        data = self.fileobj.read(len(self.global_magic))
        if data != self.global_magic:
            raise ValueError("ar global header not found")
        while True:
            file_header = self.fileobj.read(60)
            if not file_header:
                raise ValueError("end of archive found")
            parts = struct.unpack("16s 12s 6s 6s 8s 10s 2s", file_header)
            parts = [p.rstrip(" ") for p in parts]
            if parts.pop() != self.file_magic:
                print(repr(file_header))
                raise ValueError("ar file header not found")
            name = parts[0]
            length = int(parts[5])
            if self.membertest(name):
                self.remaining = length
                return name, length
            self.skip(length + length % 2)

    def read(self, length=None):
        if length is None:
            length = self.remaining
        else:
            length = min(self.remaining, length)
        data = self.fileobj.read(length)
        self.remaining -= len(data)
        return data

    def close(self):
        self.fileobj.close()

class XzStream(object):
    blocksize = 65536

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.decomp = lzma.LZMADecompressor()
        self.buff = b""

    def read(self, length):
        data = True
        while True:
            if len(self.buff) >= length:
                ret = self.buff[:length]
                self.buff = self.buff[length:]
                return ret
            elif not data: # read EOF in last iteration
                ret = self.buff
                self.buff = b""
                return ret
            data = self.fileobj.read(self.blocksize)
            if data:
                self.buff += self.decomp.decompress(data)
            else:
                self.buff += self.decomp.flush()

def hash_file(hashobj, filelike, blocksize=65536):
    data = filelike.read(blocksize)
    while data:
        hashobj.update(data)
        data = filelike.read(blocksize)
    return hashobj

def get_hashes(filelike):
    af = ArReader(filelike, lambda name: name.startswith("data.tar"))
    name, membersize = af.skiptillmember()
    if name == "data.tar.gz":
        tf = tarfile.open(fileobj=af, mode="r|gz")
    elif name == "data.tar.bz2":
        tf = tarfile.open(fileobj=af, mode="r|bz2")
    elif name == "data.tar.xz":
        zf = XzStream(af)
        tf = tarfile.open(fileobj=zf, mode="r|")
    else:
        raise ValueError("unsupported compression %r" % name)
    for elem in tf:
        if elem.size == 0: # boring
            continue
        if not elem.isreg(): # excludes hard links as well
            continue
        hasher = hash_file(hashlib.sha512(), tf.extractfile(elem))
        yield (elem.name, elem.size, hasher.hexdigest())

def main():
    filename = sys.argv[1]
    match = re.match("(?:.*/)?(?P<name>[^_]+)_(?P<version>[^_]+)_(?P<architecture>[^_.]+)\\.deb$", filename)
    package, version, architecture = match.groups()
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()

    cur.execute("SELECT version FROM content WHERE package = ?;", (package,))
    versions = [tpl[0] for tpl in cur.fetchall()]
    versions.append(version)
    versions.sort(cmp=apt_pkg.version_compare)
    if versions[-1] != version:
        return # not the newest version

    cur.execute("DELETE FROM content WHERE package = ?;", (package,))
    #cur.execute("DELETE FROM content WHERE package = ? AND version = ? AND architecture = ?;",
    #        (package, version, architecture))
    with open(filename) as pkg:
        for name, size, hexhash in get_hashes(pkg):
            name = name.decode("utf8")
            cur.execute("INSERT INTO content (package, version, architecture, filename, size, hash) VALUES (?, ?, ?, ?, ?, ?);",
                    (package, version, architecture, name, size, hexhash))
    db.commit()

if __name__ == "__main__":
    main()
