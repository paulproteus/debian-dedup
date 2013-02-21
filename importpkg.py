#!/usr/bin/python
"""
CREATE TABLE package (package TEXT PRIMARY KEY, version TEXT, architecture TEXT);
CREATE TABLE content (package TEXT, filename TEXT, size INTEGER, function TEXT, hash TEXT, FOREIGN KEY (package) REFERENCES package(package));
CREATE TABLE dependency (package TEXT, required TEXT, FOREIGN KEY (package) REFERENCES package(package), FOREIGN KEY (required) REFERENCES package(package));
CREATE INDEX content_package_index ON content (package);
CREATE INDEX content_hash_index ON content (hash);
"""

import hashlib
import sqlite3
import struct
import sys
import tarfile
import zlib

from debian.debian_support import version_compare
from debian import deb822
import lzma

class ArReader(object):
    global_magic = b"!<arch>\n"
    file_magic = b"`\n"

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.remaining = None
        self.padding = 0

    def skip(self, length):
        while length:
            data = self.fileobj.read(min(4096, length))
            if not data:
                raise ValueError("archive truncated")
            length -= len(data)

    def read_magic(self):
        data = self.fileobj.read(len(self.global_magic))
        if data != self.global_magic:
            raise ValueError("ar global header not found")
        self.remaining = 0

    def read_entry(self):
        self.skip_current_entry()
        if self.padding:
            if self.fileobj.read(1) != '\n':
                raise ValueError("missing ar padding")
            self.padding = 0
        file_header = self.fileobj.read(60)
        if not file_header:
            raise EOFError("end of archive found")
        parts = struct.unpack("16s 12s 6s 6s 8s 10s 2s", file_header)
        parts = [p.rstrip(" ") for p in parts]
        if parts.pop() != self.file_magic:
            raise ValueError("ar file header not found")
        self.remaining = int(parts[5])
        self.padding = self.remaining % 2
        return parts[0] # name

    def skip_current_entry(self):
        self.skip(self.remaining)
        self.remaining = 0

    def read(self, length=None):
        if length is None:
            length = self.remaining
        else:
            length = min(self.remaining, length)
        data = self.fileobj.read(length)
        self.remaining -= len(data)
        return data

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

class MultiHash(object):
    def __init__(self, *hashes):
        self.hashes = hashes

    def update(self, data):
        for hasher in self.hashes:
            hasher.update(data)

class HashBlacklist(object):
    def __init__(self, hasher, blacklist=set()):
        self.hasher = hasher
        self.blacklist = blacklist
        self.update = self.hasher.update
        self.name = hasher.name

    def hexdigest(self):
        digest = self.hasher.hexdigest()
        if digest in self.blacklist:
            return None
        return digest

class GzipDecompressor(object):
    def __init__(self):
        self.inbuffer = b""
        self.decompressor = None # zlib.decompressobj(-zlib.MAX_WBITS)

    def decompress(self, data):
        if self.decompressor:
            data = self.decompressor.decompress(data)
            if not self.decompressor.unused_data:
                return data
            unused_data = self.decompressor.unused_data
            self.decompressor = None
            return data + self.decompress(unused_data)
        self.inbuffer += data
        skip = 10
        if len(self.inbuffer) < skip:
            return b""
        if not self.inbuffer.startswith(b"\037\213\010"):
            raise ValueError("gzip magic not found")
        flag = ord(self.inbuffer[3])
        if flag & 4:
            if len(self.inbuffer) < skip + 2:
                return b""
            length, = struct.unpack("<H", self.inbuffer[skip:skip+2])
            skip += 2 + length
        for field in (8, 16):
            if flag & field:
                length = self.inbuffer.find("\0", skip)
                if length < 0:
                    return b""
                skip = length + 1
        if flag & 2:
            skip += 2
        if len(self.inbuffer) < skip:
            return b""
        data = self.inbuffer[skip:]
        self.inbuffer = b""
        self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        return self.decompress(data)

    @property
    def unused_data(self):
        if self.decompressor:
            return self.decompressor.unused_data
        else:
            return self.inbuffer

    def flush(self):
        if not self.decompressor:
            return b""
        return self.decompressor.flush()

    def copy(self):
        new = GzipDecompressor()
        new.inbuffer = self.inbuffer
        if self.decompressor:
            new.decompressor = self.decompressor.copy()
        return new

class DecompressedHash(object):
    def __init__(self, decompressor, hashobj):
        self.decompressor = decompressor
        self.hashobj = hashobj

    def update(self, data):
        self.hashobj.update(self.decompressor.decompress(data))

    def hexdigest(self):
        if not hasattr(self.decompressor, "flush"):
            return self.hashobj.hexdigest()
        tmpdecomp = self.decompressor.copy()
        data = tmpdecomp.flush()
        tmphash = self.hashobj.copy()
        tmphash.update(data)
        return tmphash.hexdigest()

class SuppressingHash(object):
    def __init__(self, hashobj, exceptions=()):
        self.hashobj = hashobj
        self.exceptions = exceptions

    def update(self, data):
        if self.hashobj:
            try:
                self.hashobj.update(data)
            except self.exceptions:
                self.hashobj = None

    def hexdigest(self):
        if self.hashobj:
            try:
                return self.hashobj.hexdigest()
            except self.exceptions:
                self.hashobj = None
        return None

def hash_file(hashobj, filelike, blocksize=65536):
    data = filelike.read(blocksize)
    while data:
        hashobj.update(data)
        data = filelike.read(blocksize)
    return hashobj

boring_sha512_hashes = set((
    # ""
    "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
    # "\n"
    "be688838ca8686e5c90689bf2ab585cef1137c999b48c70b92f67a5c34dc15697b5d11c982ed6d71be1e1e7f7b4e0733884aa97c3f7a339a8ed03577cf74be09"))

def sha512_nontrivial():
    return HashBlacklist(hashlib.sha512(), boring_sha512_hashes)

def gziphash():
    hashobj = DecompressedHash(GzipDecompressor(), hashlib.sha512())
    hashobj = SuppressingHash(hashobj, (ValueError, zlib.error))
    hashobj.name = "gzip_sha512"
    return HashBlacklist(hashobj, boring_sha512_hashes)

def get_hashes(tar):
    for elem in tar:
        if not elem.isreg(): # excludes hard links as well
            continue
        hasher = MultiHash(sha512_nontrivial(), gziphash())
        hasher = hash_file(hasher, tar.extractfile(elem))
        for hashobj in hasher.hashes:
            hashvalue = hashobj.hexdigest()
            if hashvalue:
                yield (elem.name, elem.size, hashobj.name, hashvalue)

def process_package(db, filelike):
    cur = db.cursor()
    af = ArReader(filelike)
    af.read_magic()
    state = "start"
    while True:
        try:
            name = af.read_entry()
        except EOFError:
            break
        if name == "control.tar.gz":
            if state != "start":
                raise ValueError("unexpected control.tar.gz")
            state = "control"
            tf = tarfile.open(fileobj=af, mode="r|gz")
            for elem in tf:
                if elem.name != "./control":
                    continue
                if state != "control":
                    raise ValueError("duplicate control file")
                state = "control_file"
                control = tf.extractfile(elem).read()
                control = deb822.Packages(control)
                package = control["package"].encode("ascii")
                version = control["version"].encode("ascii")
                architecture = control["architecture"].encode("ascii")

                cur.execute("SELECT version FROM package WHERE package = ?;",
                            (package,))
                row = cur.fetchone()
                if row and version_compare(row[0], version) > 0:
                    return # already seen a newer package

                cur.execute("DELETE FROM package WHERE package = ?;",
                            (package,))
                cur.execute("DELETE FROM content WHERE package = ?;",
                            (package,))
                cur.execute("INSERT INTO package (package, version, architecture) VALUES (?, ?, ?);",
                            (package, version, architecture))
                depends = control.relations.get("depends", [])
                depends = set(dep[0]["name"].encode("ascii")
                              for dep in depends if len(dep) == 1)
                cur.execute("DELETE FROM dependency WHERE package = ?;",
                            (package,))
                cur.executemany("INSERT INTO dependency (package, required) VALUES (?, ?);",
                                ((package, dep) for dep in depends))
                break
            continue
        elif name == "data.tar.gz":
            tf = tarfile.open(fileobj=af, mode="r|gz")
        elif name == "data.tar.bz2":
            tf = tarfile.open(fileobj=af, mode="r|bz2")
        elif name == "data.tar.xz":
            zf = XzStream(af)
            tf = tarfile.open(fileobj=zf, mode="r|")
        else:
            continue
        if state != "control_file":
            raise ValueError("missing control file")
        for name, size, function, hexhash in get_hashes(tf):
            cur.execute("INSERT INTO content (package, filename, size, function, hash) VALUES (?, ?, ?, ?, ?);",
                        (package, name.decode("utf8"), size, function, hexhash))
        db.commit()
        return
    raise ValueError("data.tar not found")

def main():
    db = sqlite3.connect("test.sqlite3")
    process_package(db, sys.stdin)

if __name__ == "__main__":
    main()
