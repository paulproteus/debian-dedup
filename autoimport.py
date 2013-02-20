#!/usr/bin/python

import gzip
import io
import sqlite3
import subprocess
import sys
import urllib

from debian import deb822

def main():
    urlbase = sys.argv[1]
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()

    cur.execute("SELECT package, version, architecture FROM package;")
    knownpkgs = dict((row[0], dict(package=row[0], version=row[1],
                                   architecture=row[2]))
                     for row in cur.fetchall())
    pkglist = urllib.urlopen(urlbase + "/dists/sid/main/binary-amd64/Packages.gz").read()
    pkglist = gzip.GzipFile(fileobj=io.BytesIO(pkglist)).read()
    distpkgs = dict()
    for pkg in deb822.Packages.iter_paragraphs(io.BytesIO(pkglist)):
        name = pkg["Package"]
        distpkgs[name] = pkg
        if pkg["Version"] == knownpkgs.get(name, dict(version=()))["version"]:
            continue
        pkgurl = "%s/%s" % (urlbase, pkg["Filename"])
        print("importing %s" % name)
        dl = subprocess.Popen(["curl", "-s", pkgurl], stdout=subprocess.PIPE)
        imp = subprocess.Popen("./test.py", stdin=dl.stdout)
        if dl.wait():
            print("curl failed")
        if imp.wait():
            print("import failed")
    
    cur.execute("PRAGMA foreign_keys=1;")
    cur.executemany("DELETE FROM package WHERE package = ?;",
                    ((pkg,) for pkg in set(knownpkgs) - set(distpkgs)))
    db.commit()

if __name__ == "__main__":
    main()
