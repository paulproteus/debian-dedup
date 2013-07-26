#!/usr/bin/python

import sqlite3

from dedup.utils import fetchiter

def add_values(cursor, insert_key, files, size):
    cursor.execute("UPDATE sharing SET files = files + ?, size = size + ? WHERE pid1 = ? AND pid2 = ? AND fid1 = ? AND fid2 = ?;",
                   (files, size) + insert_key)
    if cursor.rowcount > 0:
        return
    cursor.execute("INSERT INTO sharing (pid1, pid2, fid1, fid2, files, size) VALUES (?, ?, ?, ?, ?, ?);",
                   insert_key + (files, size))

def compute_pkgdict(rows):
    pkgdict = dict()
    for pid, _, filename, size, fid in rows:
        funcdict = pkgdict.setdefault(pid, {})
        funcdict.setdefault(fid, []).append((size, filename))
    return pkgdict

def process_pkgdict(cursor, pkgdict):
    for pid1, funcdict1 in pkgdict.items():
        for fid1, files in funcdict1.items():
            numfiles = len(files)
            size = sum(entry[0] for entry in files)
            for pid2, funcdict2 in pkgdict.items():
                if pid1 == pid2:
                    pkgnumfiles = numfiles - 1
                    pkgsize = size - min(entry[0] for entry in files)
                    if pkgnumfiles == 0:
                        continue
                else:
                    pkgnumfiles = numfiles
                    pkgsize = size
                for fid2 in funcdict2.keys():
                    insert_key = (pid1, pid2, fid1, fid2)
                    add_values(cursor, insert_key, pkgnumfiles, pkgsize)

def main():
    db = sqlite3.connect("test.sqlite3")
    cur = db.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("DELETE FROM sharing;")
    cur.execute("DELETE FROM duplicate;")
    cur.execute("DELETE FROM issue;")
    readcur = db.cursor()
    readcur.execute("SELECT hash FROM hash GROUP BY hash HAVING count(*) > 1;")
    for hashvalue, in fetchiter(readcur):
        cur.execute("SELECT content.pid, content.id, content.filename, content.size, hash.fid FROM hash JOIN content ON hash.cid = content.id WHERE hash = ?;",
                    (hashvalue,))
        rows = cur.fetchall()
        print("processing hash %s with %d entries" % (hashvalue, len(rows)))
        pkgdict = compute_pkgdict(rows)
        cur.executemany("INSERT OR IGNORE INTO duplicate (cid) VALUES (?);",
                        [(row[1],) for row in rows])
        process_pkgdict(cur, pkgdict)
    cur.execute("INSERT INTO issue (cid, issue) SELECT content.id, 'file named something.gz is not a valid gzip file' FROM content WHERE content.filename LIKE '%.gz' AND NOT EXISTS (SELECT 1 FROM hash WHERE hash.cid = content.id AND hash.function = 'gzip_sha512');")
    cur.execute("INSERT INTO issue (cid, issue) SELECT content.id, 'png image not named something.png' FROM content JOIN hash ON content.id = hash.cid WHERE function = 'image_sha512' AND lower(filename) NOT LIKE '%.png';")
    db.commit()

if __name__ == "__main__":
    main()
