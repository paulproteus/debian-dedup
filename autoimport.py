#!/usr/bin/python

import gzip
import io
import sqlite3
import subprocess
import sys
import urllib

from debian import deb822
from debian.debian_support import version_compare

def main():
    urlbase = sys.argv[1]
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("SELECT package, version FROM package;")
    knownpkgs = dict((row[0], row[1]) for row in cur.fetchall())

    pkglist = urllib.urlopen(urlbase + "/dists/sid/main/binary-amd64/Packages.gz").read()
    pkglist = gzip.GzipFile(fileobj=io.BytesIO(pkglist)).read()
    distpkgs = set()
    for pkg in deb822.Packages.iter_paragraphs(io.BytesIO(pkglist)):
        name = pkg["Package"]
        distpkgs.add(name)
        if name in knownpkgs and \
                version_compare(pkg["Version"], knownpkgs[name]) <= 0:
            continue
        pkgurl = "%s/%s" % (urlbase, pkg["Filename"])
        print("importing %s" % name)
        dl = subprocess.Popen(["curl", "-s", pkgurl], stdout=subprocess.PIPE)
        imp = subprocess.Popen("./importpkg.py", stdin=dl.stdout)
        if imp.wait():
            print("import failed")
        if dl.wait():
            print("curl failed")
    
    delpkgs = set(knownpkgs) - distpkgs
    print("clearing packages %s" % " ".join(delpkgs))
    cur.executemany("DELETE FROM sharing WHERE package1 = ?",
                    ((pkg,) for pkg in delpkgs))
    cur.executemany("DELETE FROM sharing WHERE package2 = ?",
                    ((pkg,) for pkg in delpkgs))
    cur.executemany("DELETE FROM content WHERE package = ?;",
                    ((pkg,) for pkg in delpkgs))
    cur.executemany("DELETE FROM dependency WHERE package = ?;",
                    ((pkg,) for pkg in delpkgs))
    cur.executemany("DELETE FROM package WHERE package = ?;",
                    ((pkg,) for pkg in delpkgs))
    db.commit()

if __name__ == "__main__":
    main()
