CREATE TABLE package (package TEXT PRIMARY KEY, version TEXT, architecture TEXT);
CREATE TABLE content (package TEXT, filename TEXT, size INTEGER, function TEXT, hash TEXT, FOREIGN KEY (package) REFERENCES package(package));
CREATE TABLE dependency (package TEXT, required TEXT, FOREIGN KEY (package) REFERENCES package(package), FOREIGN KEY (required) REFERENCES package(package));
CREATE INDEX content_package_index ON content (package);
CREATE INDEX content_hash_index ON content (hash);
