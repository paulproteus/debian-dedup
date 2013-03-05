#!/usr/bin/python

import hashlib
import sqlite3
import struct
import sys
import tarfile
import zlib

from debian.debian_support import version_compare
from debian import deb822
import lzma

from dedup.hashing import HashBlacklist, DecompressedHash, SuppressingHash, hash_file
from dedup.compression import GzipDecompressor, DecompressedStream
from dedup.image import ImageHash

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

class MultiHash(object):
    def __init__(self, *hashes):
        self.hashes = hashes

    def update(self, data):
        for hasher in self.hashes:
            hasher.update(data)

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

def imagehash():
    hashobj = ImageHash(hashlib.sha512())
    hashobj = SuppressingHash(hashobj, (ValueError,))
    hashobj.name = "image_sha512"
    return hashobj

def get_hashes(tar):
    for elem in tar:
        if not elem.isreg(): # excludes hard links as well
            continue
        hasher = MultiHash(sha512_nontrivial(), gziphash(), imagehash())
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
                try:
                    source = control["source"].encode("ascii").split()[0]
                except KeyError:
                    source = package
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
                cur.execute("DELETE FROM source WHERE package = ?;",
                            (package,))
                cur.execute("INSERT INTO source (source, package) VALUES (?, ?);",
                            (source, package))
                break
            continue
        elif name == "data.tar.gz":
            tf = tarfile.open(fileobj=af, mode="r|gz")
        elif name == "data.tar.bz2":
            tf = tarfile.open(fileobj=af, mode="r|bz2")
        elif name == "data.tar.xz":
            zf = DecompressedStream(af, lzma.LZMADecompressor())
            tf = tarfile.open(fileobj=zf, mode="r|")
        else:
            continue
        if state != "control_file":
            raise ValueError("missing control file")
        for name, size, function, hexhash in get_hashes(tf):
            try:
                name = name.decode("utf8")
            except UnicodeDecodeError:
                print("warning: skipping filename with encoding error")
                continue # skip files with non-utf8 encoding for now
            cur.execute("INSERT INTO content (package, filename, size, function, hash) VALUES (?, ?, ?, ?, ?);",
                        (package, name, size, function, hexhash))
        db.commit()
        return
    raise ValueError("data.tar not found")

def main():
    db = sqlite3.connect("test.sqlite3")
    process_package(db, sys.stdin)

if __name__ == "__main__":
    main()
