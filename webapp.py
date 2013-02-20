#!/usr/bin/python

import sqlite3
from wsgiref.simple_server import make_server

import jinja2
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.routing import Map, Rule
from werkzeug.wrappers import Request, Response

jinjaenv = jinja2.Environment()

def format_size(size):
    assert isinstance(size, int)
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

jinjaenv.filters["format_size"] = format_size

package_template = jinjaenv.from_string(
"""<html><head><title>duplication of {{ package|e }}</title></head>
<body><h1>{{ package|e }}</h1>
<p>Version: {{ version|e }}</p>
<p>Architecture: {{ architecture|e }}</p>
<p>Number of files: {{ num_files }}</p>
<p>Total size: {{ total_size|format_size }}</p>
{%- if shared -%}
    {%- for function, sharing in shared.items() -%}
        <h3>sharing with respect to {{ function }}</h3>
        <table border='1'><tr><th>package</th><th>files shared</th><th>data shared</th></tr>
        {%- for entry in sharing|sort(attribute="savable", reverse=true) -%}
            <tr><td>{% if entry.package %}<a href="{{ entry.package|e }}">{{ entry.package|e }}</a>{% else %}self{% endif %}
                <a href="../compare/{{ package|e }}/{{ entry.package|default(package, true)|e }}">compare</a></td>
            <td>{{ entry.duplicate }} ({{ (100 * entry.duplicate / num_files)|int }}%)</td>
            <td>{{ entry.savable|format_size }} ({{ (100 * entry.savable / total_size)|int }}%)</td></tr>
        {%- endfor -%}
        </table>
    {%- endfor -%}
{%- endif -%}
{{ content }}
</body></html>""")

detail_template = jinjaenv.from_string(
"""<html><head><title>sharing between {{ details1.package|e }} and {{ details2.package|e }}</title></head>
<body><h1>{{ details1.package|e }} &lt;-&gt; {{ details2.package|e }}</h1>
{%- if shared -%}
<table border='1'><tr><th>size</th><th>filename in {{ details1.package|e }}</th><th>filename in {{ details2.package|e }}</th><th>hash functions</th></tr>
    {%- for entry in shared|sort(attribute="size", reverse=true) -%}
        <tr><td>{{ entry.size|format_size }}</td><td>{{ entry.filename1 }}</td><td>{{ entry.filename2 }}</td><td>
        {%- for function, hashvalue in entry.functions.items() %}<a href="../../hash/{{ function|e }}/{{ hashvalue|e }}">{{ function|e }}</a> {% endfor %}</td></tr>
    {%- endfor -%}
</table>
{%- endif -%}
</body></html>""")

hash_template = jinjaenv.from_string(
"""<html><head><title>information on {{ function|e }} hash {{ hashvalue|e }}</title></head>
<body><h1>{{ function|e }} {{ hashvalue|e }}</h1>
<table border='1'><tr><th>package</th><th>filename</th><th>size</th></tr>
{%- for entry in entries -%}
    <tr><td><a href="../../binary/{{ entry.package|e }}">{{ entry.package|e }}</a></td>
    <td>{{ entry.filename|e }}</td><td>{{ entry.size|format_size }}</td></tr>
{%- endfor -%}
</table>
</body></html>""")

class Application(object):
    def __init__(self):
        self.db = sqlite3.connect("test.sqlite3")
        self.cur = self.db.cursor()
        self.routingmap = Map([
            Rule("/binary/<package>", methods=("GET",), endpoint="package"),
            Rule("/compare/<package1>/<package2>", methods=("GET",), endpoint="detail"),
            Rule("/hash/<function>/<hashvalue>", methods=("GET",), endpoint="hash"),
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
            raise NotFound()
        except HTTPException as e:
            return e

    def get_details(self, package):
        self.cur.execute("SELECT version, architecture FROM package WHERE package = ?;",
                         (package,))
        row = self.cur.fetchone()
        if not row:
            raise NotFound()
        version, architecture = row
        details = dict(package=package,
                       version=version,
                       architecture=architecture)
        self.cur.execute("SELECT count(filename), sum(size) FROM content WHERE package = ?;",
                         (package,))
        num_files, total_size = self.cur.fetchone()
        details.update(dict(num_files=num_files, total_size=total_size))
        return details

    def show_package(self, package):
        params = self.get_details(package)

        shared = dict()
        self.cur.execute("SELECT a.filename, a.function, a.hash, a.size, b.package FROM content AS a JOIN content AS b ON a.function = b.function AND a.hash = b.hash WHERE a.package = ? AND (a.filename != b.filename OR b.package != ?);",
                         (package, package))
        for afile, function, hashval, size, bpkg in self.cur.fetchall():
            pkgdict = shared.setdefault(function, dict())
            hashdict = pkgdict.setdefault(bpkg, dict())
            fileset = hashdict.setdefault(hashval, (size, set()))[1]
            fileset.add(afile)
        sharedstats = {}
        if shared:
            for function, sharing in shared.items():
                sharedstats[function] = list()
                mapping = sharing.pop(package, dict())
                if mapping:
                    duplicate = sum(len(files) for _, files in mapping.values())
                    savable = sum(size * (len(files) - 1) for size, files in mapping.values())
                    sharedstats[function].append(dict(package=None, duplicate=duplicate, savable=savable))
                for pkg, mapping in sharing.items():
                    duplicate = sum(len(files) for _, files in mapping.values())
                    savable = sum(size * len(files) for size, files in mapping.values())
                    sharedstats[function].append(dict(package=pkg, duplicate=duplicate, savable=savable))

        params["shared"] = sharedstats
        return Response(package_template.render(**params).encode("utf8"),
                        content_type="text/html")

    def show_detail(self, package1, package2):
        details1 = self.get_details(package1)
        details2 = self.get_details(package2)

        self.cur.execute("SELECT a.filename, b.filename, a.size, a.function, a.hash FROM content AS a JOIN content AS b ON a.function = b.function AND a.hash = b.hash WHERE a.package = ? AND b.package = ? AND a.filename != b.filename;",
                        (package1, package2))
        shared = dict()
        for filename1, filename2, size, function, hashvalue in self.cur.fetchall():
            shared.setdefault((filename1, filename2, size), dict())[function] = hashvalue
        shared = [dict(filename1=filename1, filename2=filename2, size=size,
                       functions=functions)
                  for (filename1, filename2, size), functions in shared.items()]
        params = dict(
            details1=details1,
            details2=details2,
            shared=shared)
        return Response(detail_template.render(**params).encode("utf8"),
                        content_type="text/html")

    def show_hash(self, function, hashvalue):
        self.cur.execute("SELECT package, filename, size FROM content WHERE function = ? AND hash = ?;",
                         (function, hashvalue))
        entries = [dict(package=package, filename=filename, size=size)
                   for package, filename, size in self.cur.fetchall()]
        if not entries:
            raise NotFound()
        params = dict(function=function, hashvalue=hashvalue, entries=entries)
        return Response(hash_template.render(**params).encode("utf8"),
                        content_type="text/html")

def main():
    app = Application()
    #app = DebuggedApplication(app, evalex=True)
    make_server("0.0.0.0", 8800, app).serve_forever()

if __name__ == "__main__":
    main()
