import struct

class ArReader(object):
    """Streaming AR file reader. After constructing an object, you usually
    call read_magic once. Then you call read_entry in a loop and use the
    ArReader object as file-like only providing read() to read the respective
    file contents until you get EOFError from read_entry.
    """
    global_magic = b"!<arch>\n"
    file_magic = b"`\n"

    def __init__(self, fileobj):
        """
        @param fileobj: a file-like object providing nothing but read(length)
        """
        self.fileobj = fileobj
        self.remaining = None
        self.padding = 0

    def read_magic(self):
        """Consume the AR magic marker at the beginning of an AR file. You
        must not call any other method before calling this method.
        @raises ValueError: if the magic is not found
        """
        data = self.fileobj.read(len(self.global_magic))
        if data != self.global_magic:
            raise ValueError("ar global header not found")
        self.remaining = 0

    def read_entry(self):
        """Read the next file header, return the filename and record the
        length of the next file, so that the read method can be used to
        exhaustively read the current file.
        @rtype: bytes
        @returns: the name of the next file
        @raises ValueError: if the data format is wrong
        @raises EOFError: when the end f the stream is reached
        """
        self.skip_current_entry()
        if self.padding:
            if self.fileobj.read(1) != b'\n':
                raise ValueError("missing ar padding")
            self.padding = 0
        file_header = self.fileobj.read(60)
        if not file_header:
            raise EOFError("end of archive found")
        parts = struct.unpack("16s 12s 6s 6s 8s 10s 2s", file_header)
        parts = [p.rstrip(" ") for p in parts]
        if parts.pop() != self.file_magic:
            raise ValueError("ar file header not found")
        self.remaining = int(parts[5])
        self.padding = self.remaining % 2
        return parts[0] # name

    def skip_current_entry(self):
        """Skip the remainder of the current file. This method must not be
        called before calling read_entry.
        @raises ValueError: if the archive appears truncated
        """
        while self.remaining:
            data = self.fileobj.read(min(4096, self.remaining))
            if not data:
                raise ValueError("archive truncated")
            self.remaining -= len(data)

    def read(self, length=None):
        """
        @type length: int or None
        @param length: number of bytes to read from the current file
        @rtype: bytes
        @returns: length or fewer bytes from the current file
        """
        if length is None:
            length = self.remaining
        else:
            length = min(self.remaining, length)
        data = self.fileobj.read(length)
        self.remaining -= len(data)
        return data
