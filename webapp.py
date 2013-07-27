#!/usr/bin/python

import datetime
import os.path
import sqlite3
from wsgiref.simple_server import make_server

import jinja2
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.routing import Map, Rule, RequestRedirect
from werkzeug.wrappers import Request, Response
from werkzeug.wsgi import SharedDataMiddleware

from dedup.utils import fetchiter

hash_functions = [
        ("sha512", "sha512"),
        ("image_sha512", "image_sha512"),
        ("gzip_sha512", "gzip_sha512"),
        ("sha512", "gzip_sha512"),
        ("gzip_sha512", "sha512")]

jinjaenv = jinja2.Environment(loader=jinja2.PackageLoader("dedup", "templates"))

def format_size(size):
    size = float(size)
    fmt = "%d B"
    if size >= 1024:
        size /= 1024
        fmt = "%.1f KB"
    if size >= 1024:
        size /= 1024
        fmt = "%.1f MB"
    if size >= 1024:
        size /= 1024
        fmt = "%.1f GB"
    return fmt % size

def function_combination(function1, function2):
    if function1 == function2:
        return function1
    return "%s -> %s" % (function1, function2)

# Workaround for jinja bug #59 (broken filesizeformat)
jinjaenv.filters["filesizeformat"] = format_size

base_template = jinjaenv.get_template("base.html")
package_template = jinjaenv.get_template("binary.html")
detail_template = jinjaenv.get_template("compare.html")
hash_template = jinjaenv.get_template("hash.html")
index_template = jinjaenv.get_template("index.html")
source_template = jinjaenv.get_template("source.html")

def encode_and_buffer(iterator):
    buff = b""
    for elem in iterator:
        buff += elem.encode("utf8")
        if len(buff) >= 2048:
            yield buff
            buff = b""
    if buff:
        yield buff

def html_response(unicode_iterator, max_age=24 * 60 * 60):
    resp = Response(encode_and_buffer(unicode_iterator), mimetype="text/html")
    resp.cache_control.max_age = max_age
    resp.expires = datetime.datetime.now() + datetime.timedelta(seconds=max_age)
    return resp

class Application(object):
    def __init__(self, db):
        self.db = db
        self.routingmap = Map([
            Rule("/", methods=("GET",), endpoint="index"),
            Rule("/binary/<package>", methods=("GET",), endpoint="package"),
            Rule("/compare/<package1>/<package2>", methods=("GET",), endpoint="detail"),
            Rule("/hash/<function>/<hashvalue>", methods=("GET",), endpoint="hash"),
            Rule("/source/<package>", methods=("GET",), endpoint="source"),
        ])

    @Request.application
    def __call__(self, request):
        mapadapter = self.routingmap.bind_to_environ(request.environ)
        try:
            endpoint, args = mapadapter.match()
            if endpoint == "package":
                return self.show_package(args["package"])
            elif endpoint == "detail":
                return self.show_detail(args["package1"], args["package2"])
            elif endpoint == "hash":
                return self.show_hash(args["function"], args["hashvalue"])
            elif endpoint == "index":
                if not request.environ["PATH_INFO"]:
                    raise RequestRedirect(request.environ["SCRIPT_NAME"] + "/")
                return html_response(index_template.render(dict(urlroot="")))
            elif endpoint == "source":
                return self.show_source(args["package"])
            raise NotFound()
        except HTTPException as e:
            return e

    def get_details(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT id, version, architecture FROM package WHERE name = ?;",
                    (package,))
        row = cur.fetchone()
        if not row:
            raise NotFound()
        pid, version, architecture = row
        details = dict(pid=pid,
                       package=package,
                       version=version,
                       architecture=architecture)
        cur.execute("SELECT count(filename), sum(size) FROM content WHERE pid = ?;",
                    (pid,))
        num_files, total_size = cur.fetchone()
        if total_size is None:
            total_size = 0
        details.update(dict(num_files=num_files, total_size=total_size))
        return details

    def get_dependencies(self, pid):
        cur = self.db.cursor()
        cur.execute("SELECT required FROM dependency WHERE pid = ?;",
                    (pid,))
        return set(row[0] for row in fetchiter(cur))

    def cached_sharedstats(self, pid):
        cur = self.db.cursor()
        sharedstats = {}
        cur.execute("SELECT pid2, package.name, f1.name, f2.name, files, size FROM sharing JOIN package ON sharing.pid2 = package.id JOIN function AS f1 ON sharing.fid1 = f1.id JOIN function AS f2 ON sharing.fid2 = f2.id WHERE pid1 = ?;",
                    (pid,))
        for pid2, package2, func1, func2, files, size in fetchiter(cur):
            if (func1, func2) not in hash_functions:
                continue
            curstats = sharedstats.setdefault(
                    function_combination(func1, func2), list())
            if pid2 == pid:
                package2 = None
            curstats.append(dict(package=package2, duplicate=files, savable=size))
        return sharedstats

    def show_package(self, package):
        params = self.get_details(package)
        params["dependencies"] = self.get_dependencies(params["pid"])
        params["shared"] = self.cached_sharedstats(params["pid"])
        params["urlroot"] = ".."
        cur = self.db.cursor()
        cur.execute("SELECT content.filename, issue.issue FROM content JOIN issue ON content.id = issue.cid WHERE content.pid = ?;",
                    (params["pid"],))
        params["issues"] = dict(cur.fetchall())
        cur.close()
        return html_response(package_template.render(params))

    def compute_comparison(self, pid1, pid2):
        """Compute a sequence of comparison objects ordery by the size of the
        object in the first package. Each element of the sequence is a dict
        defining the following keys:
         * filenames: A set of filenames in package 1 (pid1) all referring to
           the same object.
         * size: Size of the object in bytes.
         * matches: A mapping from filenames in package 2 (pid2) to a mapping
           from hash function pairs to hash values.
        """
        cur = self.db.cursor()
        cur.execute("SELECT content.id, content.filename, content.size, hash.hash FROM content JOIN hash ON content.id = hash.cid JOIN duplicate ON content.id = duplicate.cid JOIN function ON hash.fid = function.id WHERE pid = ? AND function.name = 'sha512' ORDER BY size DESC;",
                    (pid1,))
        cursize = -1
        files = dict()
        minmatch = 2 if pid1 == pid2 else 1
        for cid, filename, size, hashvalue in fetchiter(cur):
            if cursize != size:
                for entry in files.values():
                    if len(entry["matches"]) >= minmatch:
                        yield entry
                files.clear()
                cursize = size

            if hashvalue in files:
                files[hashvalue]["filenames"].add(filename)
                continue

            entry = dict(filenames=set((filename,)), size=size, matches={})
            files[hashvalue] = entry

            cur2 = self.db.cursor()
            cur2.execute("SELECT fa.name, ha.hash, fb.name, filename FROM hash AS ha JOIN hash AS hb ON ha.hash = hb.hash JOIN content ON hb.cid = content.id JOIN function AS fa ON ha.fid = fa.id JOIN function AS fb ON hb.fid = fb.id WHERE ha.cid = ? AND pid = ?;",
                         (cid, pid2))
            for func1, hashvalue, func2, filename in fetchiter(cur2):
                entry["matches"].setdefault(filename, {})[func1, func2] = \
                        hashvalue
            cur2.close()
        cur.close()

        for entry in files.values():
            if len(entry["matches"]) >= minmatch:
                yield entry

    def show_detail(self, package1, package2):
        details1 = details2 = self.get_details(package1)
        if package1 != package2:
            details2 = self.get_details(package2)

        shared = self.compute_comparison(details1["pid"], details2["pid"])
        params = dict(
            details1=details1,
            details2=details2,
            urlroot="../..",
            shared=shared)
        return html_response(detail_template.stream(params))

    def show_hash(self, function, hashvalue):
        cur = self.db.cursor()
        cur.execute("SELECT package.name, content.filename, content.size, function.name FROM hash JOIN content ON hash.cid = content.id JOIN package ON content.pid = package.id JOIN function ON hash.fid = function.id WHERE hash = ?;",
                    (hashvalue,))
        entries = [dict(package=package, filename=filename, size=size,
                        function=otherfunc)
                   for package, filename, size, otherfunc in fetchiter(cur)
                   if (function, otherfunc) in hash_functions]
        if not entries:
            raise NotFound()
        params = dict(function=function, hashvalue=hashvalue, entries=entries,
                      urlroot="../..")
        return html_response(hash_template.render(params))

    def show_source(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT name FROM package WHERE source = ?;",
                    (package,))
        binpkgs = dict.fromkeys(pkg for pkg, in fetchiter(cur))
        if not binpkgs:
            raise NotFound
        cur.execute("SELECT p1.name, p2.name, f1.name, f2.name, sharing.files, sharing.size FROM sharing JOIN package AS p1 ON sharing.pid1 = p1.id JOIN package AS p2 ON sharing.pid2 = p2.id JOIN function AS f1 ON sharing.fid1 = f1.id JOIN function AS f2 ON sharing.fid2 = f2.id WHERE p1.source = ?;",
                    (package,))
        for binary, otherbin, func1, func2, files, size in fetchiter(cur):
            entry = dict(package=otherbin,
                         funccomb=function_combination(func1, func2),
                         duplicate=files, savable=size)
            oldentry = binpkgs.get(binary)
            if not (oldentry and oldentry["savable"] >= size):
                binpkgs[binary] = entry
        params = dict(source=package, packages=binpkgs, urlroot="..")
        return html_response(source_template.render(params))

def main():
    app = Application(sqlite3.connect("test.sqlite3"))
    staticdir = os.path.join(os.path.dirname(__file__), "static")
    app = SharedDataMiddleware(app, {"/": staticdir})
    make_server("0.0.0.0", 8800, app).serve_forever()

if __name__ == "__main__":
    main()
