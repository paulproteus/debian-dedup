CREATE TABLE package (package TEXT PRIMARY KEY, version TEXT, architecture TEXT, source TEXT);
CREATE TABLE content (id INTEGER PRIMARY KEY, package TEXT, filename TEXT, size INTEGER, FOREIGN KEY (package) REFERENCES package(package) ON DELETE CASCADE);
CREATE TABLE hash (cid INTEGER, function TEXT, hash TEXT, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE);
CREATE TABLE dependency (package TEXT, required TEXT, FOREIGN KEY (package) REFERENCES package(package) ON DELETE CASCADE);
CREATE INDEX content_package_index ON content (package);
CREATE INDEX hash_cid_index ON hash (cid);
CREATE INDEX hash_hash_index ON hash (hash);

CREATE TABLE sharing (package1 TEXT, package2 TEXT, func1 TEXT, func2 TEXT, files INTEGER, size INTEGER, FOREIGN KEY (package1) REFERENCES package(package) ON DELETE CASCADE, FOREIGN KEY (package2) REFERENCES package(package) ON DELETE CASCADE);
CREATE INDEX sharing_insert_index ON sharing (package1, package2, func1, func2);
CREATE TABLE duplicate (cid INTEGER PRIMARY KEY, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE);
