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
    cur.execute("SELECT package, version FROM package;")
    knownpkgs = dict((row[0], row[1]) for row in cur.fetchall())

    pkglist = urllib.urlopen(urlbase + "/dists/sid/main/binary-amd64/Packages.gz").read()
    pkglist = gzip.GzipFile(fileobj=io.BytesIO(pkglist)).read()
    distpkgs = set()
    for pkg in deb822.Packages.iter_paragraphs(io.BytesIO(pkglist)):
        name = pkg["Package"]
        distpkgs.add(name)
        if version_compare(pkg["Version"], knownpkgs.get(name, ())) < 0:
            continue
        pkgurl = "%s/%s" % (urlbase, pkg["Filename"])
        print("importing %s" % name)
        dl = subprocess.Popen(["curl", "-s", pkgurl], stdout=subprocess.PIPE)
        imp = subprocess.Popen("./importpkg.py", stdin=dl.stdout)
        if imp.wait():
            print("import failed")
        if dl.wait():
            print("curl failed")
    
    cur.execute("PRAGMA foreign_keys=1;")
    cur.executemany("DELETE FROM package WHERE package = ?;",
                    ((pkg,) for pkg in set(knownpkgs) - distpkgs))
    db.commit()

if __name__ == "__main__":
    main()
