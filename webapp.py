#!/usr/bin/python

import sqlite3
from wsgiref.simple_server import make_server

from werkzeug.debug import DebuggedApplication
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.routing import Map, Rule
from werkzeug.wrappers import Request, Response

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

class Application(object):
    def __init__(self):
        self.db = sqlite3.connect("test.sqlite3")
        self.cur = self.db.cursor()
        self.routingmap = Map([
            Rule("/<package>", methods=("GET",),
                 endpoint="package"),
        ])

    @Request.application
    def __call__(self, request):
        mapadapter = self.routingmap.bind_to_environ(request.environ)
        try:
            endpoint, args = mapadapter.match()
            assert endpoint == "package"
            return self.show_package(args["package"])
        except HTTPException as e:
            return e

    def show_package(self, package):
        self.cur.execute("SELECT version, architecture FROM content WHERE package = ? LIMIT 1;", (package,))
        row = self.cur.fetchone()
        if not row:
            raise NotFound()
        version, architecture = row
        self.cur.execute("SELECT count(filename) FROM content WHERE package = ?;", (package,))
        num_files = self.cur.fetchone()[0]
        self.cur.execute("SELECT sum(size) FROM content WHERE package = ?;", (package,))
        total_size = self.cur.fetchone()[0]
        content = "<p>Version: %s</p><p>Architecture: %s</p>" % (version, architecture)
        content += "<p>Number of files: %d</p>" % num_files
        content += "<p>Total size: %s</p>" % format_size(total_size)

        shared = dict()
        self.cur.execute("SELECT a.filename, a.hash, a.size, b.package FROM content AS a JOIN content AS b ON a.hash = b.hash WHERE a.package = ? AND (a.filename != b.filename OR b.package != ?);", (package, package))
        for afile, hashval, size, bpkg in self.cur.fetchall():
            shared.setdefault(bpkg, dict()).setdefault(hashval, (size, set()))[1].add(afile)
        if shared:
            sharedstats = []
            mapping = shared.pop(package, dict())
            if mapping:
                duplicate = sum(len(files) for _, files in mapping.values())
                savable = sum(size * (len(files) - 1) for size, files in mapping.values())
                sharedstats.append(("self", duplicate, savable))
            for pkg, mapping in shared.items():
                pkglink = '<a href="%s">%s</a>' % (pkg, pkg)
                duplicate = sum(len(files) for _, files in mapping.values())
                savable = sum(size * len(files) for size, files in mapping.values())
                sharedstats.append((pkglink, duplicate, savable))
            sharedstats.sort(key=lambda row: row[2], reverse=True)
            content += "<table border='1'><tr><th>package</th><th>files shared</th><th>data shared</th></tr>"
            for pkg, duplicate, savable in sharedstats:
                content += "<tr><td>%s</td><td>%d (%d%%)</td><td>%s (%d%%)</td></tr>" % (pkg, duplicate, 100. * duplicate / num_files, format_size(savable), 100. * savable / total_size)
            content += "</table>"

        r = Response(content_type="text/html")
        r.data = "<html><head><title>duplication of %(package)s</title></head><body><h1>%(package)s</h1>%(content)s</body></html>" % dict(package=package, content=content)
        return r

def main():
    app = Application()
    app = DebuggedApplication(app, evalex=True)
    make_server("localhost", 8800, app).serve_forever()

if __name__ == "__main__":
    main()
