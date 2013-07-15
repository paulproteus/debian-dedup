#!/usr/bin/python
"""This scrip takes a directory or a http base url to a mirror and imports all
packages contained. It has rather strong assumptions on the working directory.
"""

import gzip
import io
import multiprocessing
import optparse
import os
import sqlite3
import subprocess
import urllib

import concurrent.futures
from debian import deb822
from debian.debian_support import version_compare

from readyaml import readyaml

def process_http(pkgs, url):
    pkglist = urllib.urlopen(url + "/dists/sid/main/binary-amd64/Packages.gz").read()
    pkglist = gzip.GzipFile(fileobj=io.BytesIO(pkglist)).read()
    pkglist = io.BytesIO(pkglist)
    pkglist = deb822.Packages.iter_paragraphs(pkglist)
    for pkg in pkglist:
        name = pkg["Package"]
        if name in pkgs and \
                version_compare(pkgs[name]["version"], pkg["Version"]) > 0:
            continue
        pkgs[name] = dict(version=pkg["Version"],
                          filename="%s/%s" % (url, pkg["Filename"]))

def process_file(pkgs, filename):
    base = os.path.basename(filename)
    if not base.endswith(".deb"):
        raise ValueError("filename does not end in .deb")
    parts = base.split("_")
    if len(parts) != 3:
        raise ValueError("filename not in form name_version_arch.deb")
    name, version, _ = parts
    version = urllib.unquote(version)
    if name in pkgs and version_compare(pkgs[name]["version"], version) > 0:
        return
    pkgs[name] = dict(version=version, filename=filename)

def process_dir(pkgs, d):
    for entry in os.listdir(d):
        try:
            process_file(pkgs, os.path.join(d, entry))
        except ValueError:
            pass

def process_pkg(name, filename):
    print("importing %s" % filename)
    if filename.startswith("http://"):
        with open(os.path.join("tmp", name), "w") as outp:
            dl = subprocess.Popen(["curl", "-s", filename],
                                  stdout=subprocess.PIPE, close_fds=True)
            imp = subprocess.Popen(["python", "importpkg.py"], stdin=dl.stdout,
                                   stdout=outp, close_fds=True)
            if imp.wait():
                raise ValueError("importpkg failed")
            if dl.wait():
                raise ValueError("curl failed")
    else:
        with open(filename) as inp:
            with open(os.path.join("tmp", name), "w") as outp:
                subprocess.check_call(["python", "importpkg.py"], stdin=inp,
                                      stdout=outp, close_fds=True)
    print("preprocessed %s" % name)

def main():
    parser = optparse.OptionParser()
    parser.add_option("-n", "--new", action="store_true",
                      help="avoid reimporting same versions")
    parser.add_option("-p", "--prune", action="store_true",
                      help="prune packages old packages")
    options, args = parser.parse_args()
    subprocess.check_call(["mkdir", "-p", "tmp"])
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    e = concurrent.futures.ThreadPoolExecutor(multiprocessing.cpu_count())
    pkgs = {}
    for d in args:
        print("processing %s" % d)
        if d.startswith("http://"):
            process_http(pkgs, d)
        elif os.path.isdir(d):
            process_dir(pkgs, d)
        else:
            process_file(pkgs, d)

    print("reading database")
    cur.execute("SELECT name, version FROM package;")
    knownpkgs = dict((row[0], row[1]) for row in cur.fetchall())
    distpkgs = set(pkgs.keys())
    if options.new:
        for name in distpkgs:
            if name in knownpkgs and version_compare(pkgs[name]["version"],
                    knownpkgs[name]) <= 0:
                del pkgs[name]
    knownpkgs = set(knownpkgs)

    with e:
        fs = {}
        for name, pkg in pkgs.items():
            fs[e.submit(process_pkg, name, pkg["filename"])] = name

        for f in concurrent.futures.as_completed(fs.keys()):
            name = fs[f]
            if f.exception():
                print("%s failed to import: %r" % (name, f.exception()))
                continue
            inf = os.path.join("tmp", name)
            print("sqlimporting %s" % name)
            with open(inf) as inp:
                try:
                    readyaml(db, inp)
                except Exception as exc:
                    print("%s failed sql with exception %r" % (name, exc))
                else:
                    os.unlink(inf)

    if options.prune:
        delpkgs = knownpkgs - distpkgs
        print("clearing packages %s" % " ".join(delpkgs))
        cur.executemany("DELETE FROM package WHERE name = ?;",
                        ((pkg,) for pkg in delpkgs))
        # Tables content, dependency and sharing will also be pruned
        # due to ON DELETE CASCADE clauses.
        db.commit()

if __name__ == "__main__":
    main()
