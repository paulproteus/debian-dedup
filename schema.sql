CREATE TABLE package (id INTEGER PRIMARY KEY, name TEXT UNIQUE, version TEXT, architecture TEXT, source TEXT);
CREATE TABLE content (id INTEGER PRIMARY KEY, pid INTEGER, filename TEXT, size INTEGER, FOREIGN KEY (pid) REFERENCES package(id) ON DELETE CASCADE);
CREATE TABLE hash (cid INTEGER, function TEXT, hash TEXT, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE);
CREATE TABLE dependency (pid INTEGER, required TEXT, FOREIGN KEY (pid) REFERENCES package(id) ON DELETE CASCADE);
CREATE INDEX content_package_size_index ON content (pid, size);
CREATE INDEX hash_cid_index ON hash (cid);
CREATE INDEX hash_hash_index ON hash (hash);

CREATE TABLE sharing (pid1 INTEGER, pid2 INTEGER, func1 TEXT, func2 TEXT, files INTEGER, size INTEGER, FOREIGN KEY (pid1) REFERENCES package(id) ON DELETE CASCADE, FOREIGN KEY (pid2) REFERENCES package(id) ON DELETE CASCADE);
CREATE INDEX sharing_insert_index ON sharing (pid1, pid2, func1, func2);
CREATE TABLE duplicate (cid INTEGER PRIMARY KEY, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE);
