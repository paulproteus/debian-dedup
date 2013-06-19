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

jinjaenv = jinja2.Environment(loader=jinja2.FileSystemLoader("."))

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

package_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}duplication of {{ package|e }}{% endblock %}
{% block content %}<h1>{{ package|e }}</h1>
<p>Version: {{ version|e }}</p>
<p>Architecture: {{ architecture|e }}</p>
<p>Number of files: {{ num_files }}</p>
<p>Total size: {{ total_size|filesizeformat }}</p>
{%- if shared -%}
    {%- for function, sharing in shared.items() -%}
        <h3>sharing with respect to {{ function|e }}</h3>
        <table border='1'><tr><th>package</th><th>files shared</th><th>data shared</th></tr>
        {%- for entry in sharing|sort(attribute="savable", reverse=true) -%}
            <tr><td{% if not entry.package or entry.package in dependencies %} class="dependency"{% endif %}>
                {%- if entry.package %}<a href="{{ entry.package|e }}"><span class="binary-package">{{ entry.package|e }}</span></a>{% else %}self{% endif %}
                <a href="../compare/{{ package|e }}/{{ entry.package|default(package, true)|e }}">compare</a></td>
            <td>{{ entry.duplicate }} ({{ (100 * entry.duplicate / num_files)|int }}%)</td>
            <td>{{ entry.savable|filesizeformat }} ({{ (100 * entry.savable / total_size)|int }}%)</td></tr>
        {%- endfor -%}
        </table>
    {%- endfor -%}
<p>Note: Packages with yellow background are required to be installed when this package is installed.</p>
{%- endif -%}
{% endblock %}""")

detail_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}sharing between {{ details1.package|e }} and {{ details2.package|e }}{% endblock%}
{% block content %}
<h1><a href="../../binary/{{ details1.package|e }}">{{ details1.package|e }}</a> &lt;-&gt; <a href="../../binary/{{ details2.package|e }}">{{ details2.package|e }}</a></h1>
<p>Version of {{ details1.package|e }}: {{ details1.version|e }}</p>
<p>Architecture of {{ details1.package|e }}: {{ details1.architecture|e }}</p>
{%- if details1.package != details2.package -%}
<p>Version of {{ details2.package|e }}: {{ details2.version|e }}</p>
<p>Architecture of {{ details2.package|e }}: {{ details2.architecture|e }}</p>
{%- endif -%}
<table border='1'><tr><th colspan="2">{{ details1.package|e }}</th><th colspan="2">{{ details2.package|e }}</th></tr>
<tr><th>size</th><th>filename</th><th>hash functions</th><th>filename</th></tr>
{%- for entry in shared -%}
    <tr><td{% if entry.matches|length > 1 %} rowspan={{ entry.matches|length }}{% endif %}>{{ entry.size|filesizeformat }}</td><td{% if entry.matches|length > 1 %} rowspan={{ entry.matches|length }}{% endif %}>
    {%- for filename in entry.filenames %}<span class="filename">{{ filename|e }}</span>{% endfor -%}</td><td>
    {% for filename, match in entry.matches.items() -%}
        {% if not loop.first %}<tr><td>{% endif -%}
        {%- for funccomb, hashvalue in match.items() -%}
            <a href="../../hash/{{ funccomb[0]|e }}/{{ hashvalue|e }}">{{ funccomb[0]|e }}</a>
            {%- if funccomb[0] != funccomb[1] %} -&gt; <a href="../../hash/{{ funccomb[1]|e }}/{{ hashvalue|e }}">{{ funccomb[1]|e }}</a>{% endif %}
            {%- if not loop.last %}, {% endif %}
        {%- endfor -%}
        </td><td><span class="filename">{{ filename|e }}</span></td></tr>
    {%- endfor -%}
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
    <tr><td><a href="../../binary/{{ entry.package|e }}"><span class="binary-package">{{ entry.package|e }}</span></a></td>
    <td><span class="filename">{{ entry.filename|e }}</span></td><td>{{ entry.size|filesizeformat }}</td>
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
<li>To discover package shipping a particular file go to <pre>hash/sha512/&lt;hashvalue&gt;</pre> Example: <a href="hash/sha512/7633623b66b5e686bb94dd96a7cdb5a7e5ee00e87004fab416a5610d59c62badaf512a2e26e34e2455b7ed6b76690d2cd47464836d7d85d78b51d50f7e933d5c">hash/sha512/7633623b66b5e686bb94dd96a7cdb5a7e5ee00e87004fab416a5610d59c62badaf512a2e26e34e2455b7ed6b76690d2cd47464836d7d85d78b51d50f7e933d5c</a></li>
</ul>
{% endblock %}""")

source_template = jinjaenv.from_string(
"""{% extends "base.html" %}
{% block title %}overview of {{ source|e }}{% endblock %}
{% block content %}
<h1>overview of {{ source|e }}</h1>
<table border='1'><tr><th>binary from {{ source|e }}</th><th>savable</th><th>other package</th></tr>
{% for package, sharing in packages.items() %}
    <tr><td><a href="../binary/{{ package|e }}"><span class="binary-package">{{ package|e }}</span></a></td><td>
    {%- if sharing -%}
        {{ sharing.savable|filesizeformat }}</td><td><a href="../binary/{{ sharing.package|e }}"><span class="binary-package">{{ sharing.package|e }}</span></a> <a href="../compare/{{ package|e }}/{{ sharing.package|e }}">compare</a>
    {%- else -%}</td><td>{%- endif -%}
    </td></tr>
{% endfor %}
</table>
<p>Note: Not all sharing listed here. Click on binary packages with non-zero savable to see more.</p>
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
                return html_response(index_template.render(dict(urlroot="")))
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
        if total_size is None:
            total_size = 0
        details.update(dict(num_files=num_files, total_size=total_size))
        return details

    def get_dependencies(self, package):
        cur = self.db.cursor()
        cur.execute("SELECT required FROM dependency WHERE package = ?;",
                    (package,))
        return set(row[0] for row in fetchiter(cur))

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
        params["urlroot"] = ".."
        return html_response(package_template.render(params))

    def compute_comparison(self, package1, package2):
        """Compute a sequence of comparison objects ordery by the size of the
        object in the first package. Each element of the sequence is a dict
        defining the following keys:
         * filenames: A set of filenames in package1 all referring to the
           same object.
         * size: Size of the object in bytes.
         * matches: A mapping from filenames in package2 to a mapping from
           hash function pairs to hash values.
        """
        cur = self.db.cursor()
        cur.execute("SELECT id, filename, size, hash FROM content JOIN hash ON content.id = hash.cid JOIN duplicate ON content.id = duplicate.cid WHERE package = ? AND function = 'sha512' ORDER BY size DESC;",
                    (package1,))
        cursize = -1
        files = dict()
        minmatch = 2 if package1 == package2 else 1
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
            cur2.execute("SELECT ha.function, ha.hash, hb.function, filename FROM hash AS ha JOIN hash AS hb ON ha.hash = hb.hash JOIN content ON hb.cid = content.id WHERE ha.cid = ? AND package = ?;",
                         (cid, package2))
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

        shared = self.compute_comparison(package1, package2)
        params = dict(
            details1=details1,
            details2=details2,
            urlroot="../..",
            shared=shared)
        return html_response(detail_template.stream(params))

    def show_hash(self, function, hashvalue):
        cur = self.db.cursor()
        cur.execute("SELECT content.package, content.filename, content.size, hash.function FROM content JOIN hash ON content.id = hash.cid WHERE hash = ?;",
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
        cur.execute("SELECT package FROM package WHERE source = ?;",
                    (package,))
        binpkgs = dict.fromkeys(pkg for pkg, in fetchiter(cur))
        if not binpkgs:
            raise NotFound
        cur.execute("SELECT package.package, sharing.package2, sharing.func1, sharing.func2, sharing.files, sharing.size FROM package JOIN sharing ON package.package = sharing.package1 WHERE package.source = ?;",
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
