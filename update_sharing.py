#!/usr/bin/python

import sqlite3

from dedup.utils import fetchiter

def add_values(cursor, insert_key, files, size):
    cursor.execute("UPDATE sharing SET files = files + ?, size = size + ? WHERE package1 = ? AND package2 = ? AND func1 = ? AND func2 = ?;",
                   (files, size) + insert_key)
    if cursor.rowcount > 0:
        return
    cursor.execute("INSERT INTO sharing (package1, package2, func1, func2, files, size) VALUES (?, ?, ?, ?, ?, ?);",
                   insert_key + (files, size))

def compute_pkgdict(rows):
    pkgdict = dict()
    for package, filename, size, function in rows:
        funcdict = pkgdict.setdefault(package, {})
        funcdict.setdefault(function, []).append((size, filename))
    return pkgdict

def process_pkgdict(cursor, pkgdict):
    for package1, funcdict1 in pkgdict.items():
        for function1, files in funcdict1.items():
            numfiles = len(files)
            size = sum(entry[0] for entry in files)
            for package2, funcdict2 in pkgdict.items():
                if package1 == package2:
                    pkgnumfiles = numfiles - 1
                    pkgsize = size - min(entry[0] for entry in files)
                    if pkgnumfiles == 0:
                        continue
                else:
                    pkgnumfiles = numfiles
                    pkgsize = size
                for function2 in funcdict2.keys():
                    insert_key = (package1, package2, function1, function2)
                    add_values(cursor, insert_key, pkgnumfiles, pkgsize)

def main():
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("DELETE FROM sharing;")
    readcur = db.cursor()
    readcur.execute("SELECT hash FROM hash GROUP BY hash HAVING count(*) > 1;")
    for hashvalue, in fetchiter(readcur):
        cur.execute("SELECT content.package, content.filename, content.size, hash.function FROM hash JOIN content ON hash.cid = content.id WHERE hash = ?;",
                    (hashvalue,))
        rows = cur.fetchall()
        print("processing hash %s with %d entries" % (hashvalue, len(rows)))
        pkgdict = compute_pkgdict(rows)
        process_pkgdict(cur, pkgdict)
    db.commit()

if __name__ == "__main__":
    main()
