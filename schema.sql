CREATE TABLE package (id INTEGER PRIMARY KEY, name TEXT UNIQUE, version TEXT, architecture TEXT, source TEXT);
CREATE TABLE content (id INTEGER PRIMARY KEY, pid INTEGER, filename TEXT, size INTEGER, FOREIGN KEY (pid) REFERENCES package(id) ON DELETE CASCADE);
CREATE TABLE function (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, eqclass INTEGER);
INSERT INTO function (name, eqclass) VALUES ("sha512", 1), ("gzip_sha512", 1), ("png_sha512", 2), ("gif_sha512", 2);
CREATE TABLE hash (cid INTEGER, fid INTEGER NOT NULL, hash TEXT, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE, FOREIGN KEY (fid) REFERENCES function(id));
CREATE TABLE dependency (pid INTEGER, required TEXT, FOREIGN KEY (pid) REFERENCES package(id) ON DELETE CASCADE);
CREATE INDEX content_package_size_index ON content (pid, size);
CREATE INDEX hash_cid_index ON hash (cid);
CREATE INDEX hash_hash_index ON hash (hash);

CREATE TABLE sharing (
	pid1 INTEGER NOT NULL REFERENCES package(id) ON DELETE CASCADE,
	pid2 INTEGER NOT NULL REFERENCES package(id) ON DELETE CASCADE,
	fid1 INTEGER NOT NULL REFERENCES function(id),
	fid2 INTEGER NOT NULL REFERENCES function(id),
	files INTEGER,
	size INTEGER);
CREATE INDEX sharing_insert_index ON sharing (pid1, pid2, fid1, fid2);
CREATE TABLE duplicate (cid INTEGER PRIMARY KEY, FOREIGN KEY (cid) REFERENCES content(id) ON DELETE CASCADE);
CREATE TABLE issue (cid INTEGER REFERENCES content(id) ON DELETE CASCADE, issue TEXT);
