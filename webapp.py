#!/usr/bin/python

import datetime
import sqlite3
from wsgiref.simple_server import make_server

import jinja2
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.routing import Map, Rule, RequestRedirect
from werkzeug.wrappers import Request, Response

from dedup.utils import fetchiter

hash_functions = [
        ("sha512", "sha512"),
        ("image_sha512", "image_sha512"),
        ("gzip_sha512", "gzip_sha512"),
        ("sha512", "gzip_sha512"),
        ("gzip_sha512", "sha512")]

jinjaenv = jinja2.Environment(loader=jinja2.FileSystemLoader("."))

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

def function_combination(function1, function2):
    if function1 == function2:
        return function1
    return "%s -> %s" % (function1, function2)

jinjaenv.filters["format_size"] = format_size

base_template = jinjaenv.get_template("base.html")

package_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}duplication of {{ package|e }}{% endblock %}
{% block header %}<style type="text/css">.dependency { background-color: yellow; } </style>{% endblock %}
{% block content %}<h1>{{ package|e }}</h1>
<p>Version: {{ version|e }}</p>
<p>Architecture: {{ architecture|e }}</p>
<p>Number of files: {{ num_files }}</p>
<p>Total size: {{ total_size|format_size }}</p>
{%- if shared -%}
    {%- for function, sharing in shared.items() -%}
        <h3>sharing with respect to {{ function|e }}</h3>
        <table border='1'><tr><th>package</th><th>files shared</th><th>data shared</th></tr>
        {%- for entry in sharing|sort(attribute="savable", reverse=true) -%}
            <tr><td{% if not entry.package or entry.package in dependencies %} class="dependency"{% endif %}>
                {%- if entry.package %}<a href="{{ entry.package|e }}">{{ entry.package|e }}</a>{% else %}self{% endif %}
                <a href="../compare/{{ package|e }}/{{ entry.package|default(package, true)|e }}">compare</a></td>
            <td>{{ entry.duplicate }} ({{ (100 * entry.duplicate / num_files)|int }}%)</td>
            <td>{{ entry.savable|format_size }} ({{ (100 * entry.savable / total_size)|int }}%)</td></tr>
        {%- endfor -%}
        </table>
    {%- endfor -%}
{%- endif -%}
{% endblock %}""")

detail_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}sharing between {{ details1.package|e }} and {{ details2.package|e }}{% endblock%}
{% block content %}
<h1><a href="../../binary/{{ details1.package|e }}">{{ details1.package|e }}</a> &lt;-&gt; <a href="../../binary/{{ details2.package|e }}">{{ details2.package|e }}</a></h1>
<table border='1'><tr><th colspan="3">{{ details1.package|e }}</th><th colspan="3">{{ details2.package|e }}</th></tr>
<tr><th>size</th><th>filename</th><th>hash functions</th><th>size</th><th>filename</th><th>hash functions</th></tr>
    {%- for entry in shared -%}
        <tr><td>{{ entry.size1|format_size }}</td><td>{{ entry.filename1 }}</td><td>
            {%- for funccomb, hashvalue in entry.functions.items() %}<a href="../../hash/{{ funccomb[0]|e }}/{{ hashvalue|e }}">{{ funccomb[0]|e }}</a> {% endfor %}</td>
        <td>{{ entry.size2|format_size }}</td><td>{{ entry.filename2 }}</td><td>
            {%- for funccomb, hashvalue in entry.functions.items() %}<a href="../../hash/{{ funccomb[1]|e }}/{{ hashvalue|e }}">{{ funccomb[1]|e }}</a> {% endfor %}</td></tr>
    {%- endfor -%}
</table>
{% endblock %}""")

hash_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}information on {{ function|e }} hash {{ hashvalue|e }}{% endblock %}
{% block content %}
<h1>{{ function|e }} {{ hashvalue|e }}</h1>
<table border='1'><tr><th>package</th><th>filename</th><th>size</th><th>different function</th></tr>
{%- for entry in entries -%}
    <tr><td><a href="../../binary/{{ entry.package|e }}">{{ entry.package|e }}</a></td>
    <td>{{ entry.filename|e }}</td><td>{{ entry.size|format_size }}</td>
    <td>{% if function != entry.function %}{{ entry.function|e }}{% endif %}</td></tr>
{%- endfor -%}
</table>
{% endblock %}""")

index_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}Debian duplication detector{% endblock %}
{% block header %}
    <script type="text/javascript">
        function getLinkTarget() {
            var pkg = document.getElementById("pkg_name").value;
            if(pkg) {
                return "/binary/"+pkg;
            }
            return '#';
        }
        function processData() {
            var link = document.getElementById("perma_link");
            link.href = getLinkTarget();
            link.text = location.href + getLinkTarget();
        }
        window.onload = function() {
            document.getElementById('pkg_name').onkeyup = processData;
            document.getElementById("pkg_form").onsubmit = function () {
                location.href = getLinkTarget();
                return false;
            }
            processData();
            document.getElementById("form_div").style.display = '';
        }
    </script>
{% endblock %}
{% block content %}
<h1>Debian duplication detector</h1>
<ul>
<li>To inspect a particlar binary package, go to <pre>binary/&lt;packagename&gt;</pre> Example: <a href="binary/git">binary/git</a>
    <div style="display:none" id="form_div"><fieldset>
            <legend>Inspect package</legend>
            <noscript><b>This form is disfunctional when javascript is not enabled</b></noscript>
            Enter binary package to inspect - Note: Non-existing packages will result in <b>404</b>-Errors
            <form id="pkg_form">
                <label for="pkg_name">Name: <input type="text" size="30" name="pkg_name" id="pkg_name">
                <input type="submit" value="Go"> Permanent Link: <a id="perma_link" href="#"></a>
            </form>
    </fieldset></div></li>
<li>To inspect a combination of binary packages go to <pre>compare/&lt;firstpackage&gt;/&lt;secondpackage&gt;</pre> Example: <a href="compare/git/git">compare/git/git</a></li>
<li>To discover package shipping a particular file go to <pre>hash/sha512/&lt;hashvalue&gt;</pre> Example: <a href="hash/sha512/ed94df7781793f06f9426a600c1bde86397afc7b35cb3aa11b60214bd31e35ad893b53a04a2cf4676154982d7c204c4aa165d6ccdaac0170031364a05dbab3bc">hash/sha512/ed94df7781793f06f9426a600c1bde86397afc7b35cb3aa11b60214bd31e35ad893b53a04a2cf4676154982d7c204c4aa165d6ccdaac0170031364a05dbab3bc</a></li>
</ul>
{% endblock %}""")

source_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}overview of {{ source|e }}{% endblock %}
{% block content %}
<h1>overview of {{ source|e }}</h1>
<table border='1'><tr><th>binary from {{ source|e }}</th><th>savable</th><th>other package</th></tr>
{% for package, sharing in packages.items() %}
    <tr><td><a href="../binary/{{ package|e }}">{{ package|e }}</td><td>
    {%- if sharing -%}
        {{ sharing.savable|format_size }}</td><td><a href="../binary/{{ sharing.package|e }}">{{ sharing.package|e }}</a> <a href="../compare/{{ package|e }}/{{ sharing.package|e }}">compare</a>
    {%- else -%}</td><td>{%- endif -%}
    </td></tr>
{% endfor %}
</table>
{% endblock %}""")

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

def generate_shared(rows):
    """internal helper from show_detail"""
    entry = None
    for filename1, size1, func1, filename2, size2, func2, hashvalue in rows:
        funccomb = (func1, func2)
        if funccomb not in hash_functions:
            continue
        if entry and (entry["filename1"] != filename1 or
                      entry["filename2"] != filename2):
            yield entry
            entry = None
        if entry:
            funcdict = entry["functions"]
        else:
            funcdict = dict()
            entry = dict(filename1=filename1, filename2=filename2, size1=size1,
                         size2=size2, functions=funcdict)
        funcdict[funccomb] = hashvalue
    if entry:
        yield entry

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
                return html_response(index_template.stream())
            elif endpoint == "source":
                return self.show_source(args["package"])
            raise NotFound()
        except HTTPException as e:
            return e

    def get_details(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT version, architecture FROM package WHERE package = ?;",
                    (package,))
        row = cur.fetchone()
        if not row:
            raise NotFound()
        version, architecture = row
        details = dict(package=package,
                       version=version,
                       architecture=architecture)
        cur.execute("SELECT count(filename), sum(size) FROM content WHERE package = ?;",
                    (package,))
        num_files, total_size = cur.fetchone()
        details.update(dict(num_files=num_files, total_size=total_size))
        return details

    def get_dependencies(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT required FROM dependency WHERE package = ?;",
                    (package,))
        return set(row[0] for row in fetchiter(cur))

    def compute_sharedstats(self, package):
        cur = self.db.cursor()
        sharedstats = {}
        for func1, func2 in hash_functions:
            cur.execute("SELECT a.filename, a.hash, a.size, b.package FROM content AS a JOIN content AS b ON a.hash = b.hash WHERE a.package = ? AND a.function = ? AND b.function = ? AND (a.filename != b.filename OR b.package != ?);",
                        (package, func1, func2, package))
            sharing = dict()
            for afile, hashval, size, bpkg in fetchiter(cur):
                hashdict = sharing.setdefault(bpkg, dict())
                fileset = hashdict.setdefault(hashval, (size, set()))[1]
                fileset.add(afile)
            if sharing:
                sharedstats[function_combination(func1, func2)] = curstats = []
                mapping = sharing.pop(package, dict())
                if mapping:
                    duplicate = sum(len(files) for _, files in mapping.values())
                    savable = sum(size * (len(files) - 1) for size, files in mapping.values())
                    curstats.append(dict(package=None, duplicate=duplicate, savable=savable))
                for pkg, mapping in sharing.items():
                    duplicate = sum(len(files) for _, files in mapping.values())
                    savable = sum(size * len(files) for size, files in mapping.values())
                    curstats.append(dict(package=pkg, duplicate=duplicate, savable=savable))
        return sharedstats

    def cached_sharedstats(self, package):
        cur = self.db.cursor()
        sharedstats = {}
        cur.execute("SELECT package2, func1, func2, files, size FROM sharing WHERE package1 = ?;",
                    (package,))
        for package2, func1, func2, files, size in fetchiter(cur):
            if (func1, func2) not in hash_functions:
                continue
            curstats = sharedstats.setdefault(
                    function_combination(func1, func2), list())
            if package2 == package:
                package2 = None
            curstats.append(dict(package=package2, duplicate=files, savable=size))
        return sharedstats

    def show_package(self, package):
        params = self.get_details(package)
        params["dependencies"] = self.get_dependencies(package)
        params["shared"] = self.cached_sharedstats(package)
        return html_response(package_template.render(params))

    def show_detail(self, package1, package2):
        cur = self.db.cursor()
        if package1 == package2:
            details1 = details2 = self.get_details(package1)

            cur.execute("SELECT a.filename, a.size, a.function, b.filename, b.size, b.function, a.hash FROM content AS a JOIN content AS b ON a.hash = b.hash WHERE a.package = ? AND b.package = ? AND a.filename != b.filename ORDER BY a.size DESC, a.filename, b.filename;",
                        (package1, package1))
        else:
            details1 = self.get_details(package1)
            details2 = self.get_details(package2)

            cur.execute("SELECT a.filename, a.size, a.function, b.filename, b.size, b.function, a.hash FROM content AS a JOIN content AS b ON a.hash = b.hash WHERE a.package = ? AND b.package = ? ORDER BY a.size DESC, a.filename, b.filename;",
                        (package1, package2))
        shared = generate_shared(fetchiter(cur))
        # The cursor will be in use until the template is fully rendered.
        params = dict(
            details1=details1,
            details2=details2,
            shared=shared)
        return html_response(detail_template.stream(params))

    def show_hash(self, function, hashvalue):
        cur = self.db.cursor()
        cur.execute("SELECT package, filename, size, function FROM content WHERE hash = ?;",
                    (hashvalue,))
        entries = [dict(package=package, filename=filename, size=size,
                        function=otherfunc)
                   for package, filename, size, otherfunc in fetchiter(cur)
                   if (function, otherfunc) in hash_functions]
        if not entries:
            raise NotFound()
        params = dict(function=function, hashvalue=hashvalue, entries=entries)
        return html_response(hash_template.render(params))

    def show_source(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT package FROM source WHERE source = ?;",
                    (package,))
        binpkgs = dict.fromkeys(pkg for pkg, in fetchiter(cur))
        if not binpkgs:
            raise NotFound
        cur.execute("SELECT source.package, sharing.package2, sharing.func1, sharing.func2, sharing.files, sharing.size FROM source JOIN sharing ON source.package = sharing.package1 WHERE source.source = ?;",
                    (package,))
        for binary, otherbin, func1, func2, files, size in fetchiter(cur):
            entry = dict(package=otherbin,
                         funccomb=function_combination(func1, func2),
                         duplicate=files, savable=size)
            oldentry = binpkgs.get(binary)
            if not (oldentry and oldentry["savable"] >= size):
                binpkgs[binary] = entry
        params = dict(source=package, packages=binpkgs)
        return html_response(source_template.render(params))

def main():
    app = Application(sqlite3.connect("test.sqlite3"))
    #app = DebuggedApplication(app, evalex=True)
    make_server("0.0.0.0", 8800, app).serve_forever()

if __name__ == "__main__":
    main()
