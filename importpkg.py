#!/usr/bin/python
"""This tool reads a debian package from stdin and emits a yaml stream on
stdout.  It does not access a database. Therefore it can be run in parallel and
on multiple machines. The generated yaml conatins multiple documents. The first
document contains package metadata. Then a document is emitted for each file.
And finally a document consisting of the string "commit" is emitted."""

import hashlib
import sys
import tarfile
import zlib

from debian import deb822
import lzma
import yaml

from dedup.arreader import ArReader
from dedup.hashing import HashBlacklist, DecompressedHash, SuppressingHash, hash_file
from dedup.compression import GzipDecompressor, DecompressedStream
from dedup.image import ImageHash

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
        hashes = {}
        for hashobj in hasher.hashes:
            hashvalue = hashobj.hexdigest()
            if hashvalue:
                hashes[hashobj.name] = hashvalue
        yield (elem.name, elem.size, hashes)

def process_package(filelike):
    af = ArReader(filelike)
    af.read_magic()
    state = "start"
    while state not in ("finished", "skipped"):
        try:
            name = af.read_entry()
        except EOFError:
            if state != "finished":
                raise ValueError("data.tar not found")
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

                depends = control.relations.get("depends", [])
                depends = set(dep[0]["name"].encode("ascii")
                              for dep in depends if len(dep) == 1)
                yield dict(package=package, source=source, version=version,
                           architecture=architecture, depends=depends)
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
        for name, size, hashes in get_hashes(tf):
            try:
                name = name.decode("utf8")
            except UnicodeDecodeError:
                print("warning: skipping filename with encoding error")
                continue # skip files with non-utf8 encoding for now
            yield dict(name=name, size=size, hashes=hashes)
        state = "finished"
        yield "commit"

def main():
    yaml.safe_dump_all(process_package(sys.stdin), sys.stdout)

if __name__ == "__main__":
    main()
