CREATE TABLE package (package TEXT PRIMARY KEY, version TEXT, architecture TEXT);
CREATE TABLE content (package TEXT, filename TEXT, size INTEGER, function TEXT, hash TEXT, FOREIGN KEY (package) REFERENCES package(package));
CREATE TABLE dependency (package TEXT, required TEXT, FOREIGN KEY (package) REFERENCES package(package), FOREIGN KEY (required) REFERENCES package(package));
CREATE TABLE source (source TEXT, package TEXT, FOREIGN KEY (package) REFERENCES package(package));
CREATE INDEX content_package_index ON content (package);
CREATE INDEX content_hash_index ON content (hash);

CREATE TABLE sharing (package1 TEXT, package2 TEXT, func1 TEXT, func2 TEXT, files INTEGER, size INTEGER);
CREATE INDEX sharing_insert_index ON sharing (package1, package2, func1, func2);
CREATE INDEX sharing_package_index ON sharing (package1);
