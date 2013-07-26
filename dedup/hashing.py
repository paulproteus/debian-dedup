class HashBlacklist(object):
    """Turn a hashlib-like object into a hash that returns None for some
    blacklisted hashes instead of the real hash value.

    We only work with hexdigests here, so diget() disappears. The methods
    copy and update as well as the name attribute keep working as expected.
    """
    def __init__(self, hashobj, blacklist=()):
        """
        @param hashobj: a hashlib-like object
        @param blacklist: an object providing __contains__.
            hexdigest values which are contained in the blacklist
            are turned into None values
        """
        self.hashobj = hashobj
        self.blacklist = blacklist
        self.update = self.hashobj.update

    @property
    def name(self):
        return self.hashobj.name

    def hexdigest(self):
        digest = self.hashobj.hexdigest()
        if digest in self.blacklist:
            return None
        return digest

    def copy(self):
        return HashBlacklist(self.hashobj.copy(), self.blacklist)

class DecompressedHash(object):
    """Apply a decompression function before the hash. This class provides the
    hashlib interface (update, hexdigest, copy) excluding digest and name."""
    def __init__(self, decompressor, hashobj):
        """
        @param decompressor: a decompression object like bz2.BZ2Decompressor or
            lzma.LZMADecompressor. It has to provide methods decompress and
            copy as well as an unused_data attribute. It may provide a flush
            method.
        @param hashobj: a hashlib-like obj providing methods update, hexdigest
            and copy
        """
        self.decompressor = decompressor
        self.hashobj = hashobj

    def update(self, data):
        self.hashobj.update(self.decompressor.decompress(data))

    def hexdigest(self):
        if not hasattr(self.decompressor, "flush"):
            return self.hashobj.hexdigest()
        tmpdecomp = self.decompressor.copy()
        data = tmpdecomp.flush()
        tmphash = self.hashobj.copy()
        tmphash.update(data)
        return tmphash.hexdigest()

    def copy(self):
        return DecompressedHash(self.decompressor.copy(), self.hashobj.copy())

class SuppressingHash(object):
    """A hash that silences exceptions from the update and hexdigest methods of
    a hashlib-like object. If an exception has occured, hexdigest always
    returns None."""
    def __init__(self, hashobj, exceptions=()):
        """
        @param hashobj: a hashlib-like object providing methods update, copy
            and hexdigest. If a name attribute is present, it is mirrored as
            well.
        @type exceptions: tuple
        @param exceptions: exception classes to be suppressed
        """
        self.hashobj = hashobj
        self.exceptions = exceptions
        if hasattr(hashobj, "name"):
            self.name = hashobj.name

    def update(self, data):
        if self.hashobj:
            try:
                self.hashobj.update(data)
            except self.exceptions:
                self.hashobj = None

    def hexdigest(self):
        if self.hashobj:
            try:
                return self.hashobj.hexdigest()
            except self.exceptions:
                self.hashobj = None
        return None

    def copy(self):
        if self.hashobj:
            return SuppressingHash(self.hashobj.copy(), self.exceptions)
        return SuppressingHash(None, self.exceptions)

def hash_file(hashobj, filelike, blocksize=65536):
    """Feed the entire contents from the given filelike to the given hashobj.
    @param hashobj: hashlib-like object providing an update method
    @param filelike: file-like object providing read(size)
    """
    data = filelike.read(blocksize)
    while data:
        hashobj.update(data)
        data = filelike.read(blocksize)
    return hashobj

class HashedStream(object):
    """A file-like object, that supports sequential reading and hashes the
    contents on the fly."""
    def __init__(self, filelike, hashobj):
        """
        @param filelike: a file-like object, that must support the read method
        @param hashobj: a hashlib-like object providing update and hexdigest
        """
        self.filelike = filelike
        self.hashobj = hashobj

    def read(self, length):
        data = self.filelike.read(length)
        self.hashobj.update(data)
        return data

    def hexdigest(self):
        return self.hashobj.hexdigest()
