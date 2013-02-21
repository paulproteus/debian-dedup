import struct
import zlib

class GzipDecompressor(object):
    """An interface to gzip which is similar to bz2.BZ2Decompressor and
    lzma.LZMADecompressor."""
    def __init__(self):
        self.inbuffer = b""
        self.decompressor = None

    def decompress(self, data):
        """
        @raises ValueError: if no gzip magic is found
        @raises zlib.error: from zlib invocations
        """
        while True:
            if self.decompressor:
                data = self.decompressor.decompress(data)
                unused_data = self.decompressor.unused_data
                if not unused_data:
                    return data
                self.decompressor = None
                return data + self.decompress(unused_data)
            self.inbuffer += data
            skip = 10
            if len(self.inbuffer) < skip:
                return b""
            if not self.inbuffer.startswith(b"\037\213\010"):
                raise ValueError("gzip magic not found")
            flag = ord(self.inbuffer[3])
            if flag & 4:
                if len(self.inbuffer) < skip + 2:
                    return b""
                length, = struct.unpack("<H", self.inbuffer[skip:skip+2])
                skip += 2 + length
            for field in (8, 16):
                if flag & field:
                    length = self.inbuffer.find(b"\0", skip)
                    if length < 0:
                        return b""
                    skip = length + 1
            if flag & 2:
                skip += 2
            if len(self.inbuffer) < skip:
                return b""
            data = self.inbuffer[skip:]
            self.inbuffer = b""
            self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)

    @property
    def unused_data(self):
        if self.decompressor:
            return self.decompressor.unused_data
        else:
            return self.inbuffer

    def flush(self):
        """
        @raises zlib.error: from zlib invocations
        """
        if not self.decompressor:
            return b""
        return self.decompressor.flush()

    def copy(self):
        new = GzipDecompressor()
        new.inbuffer = self.inbuffer
        if self.decompressor:
            new.decompressor = self.decompressor.copy()
        return new

class DecompressedStream(object):
    """Turn a readable file-like into a decompressed file-like. Te only part
    of being file-like consists of the read(size) method in both cases."""
    blocksize = 65536

    def __init__(self, fileobj, decompressor):
        """
        @param fileobj: a file-like object providing read(size)
        @param decompressor: a bz2.BZ2Decompressor or lzma.LZMADecompressor
            like object providing methods decompress and flush and an
            attribute unused_data
        """
        self.fileobj = fileobj
        self.decompressor = decompressor
        self.buff = b""

    def read(self, length=None):
        data = True
        while True:
            if length is not None and len(self.buff) >= length:
                ret = self.buff[:length]
                self.buff = self.buff[length:]
                return ret
            elif not data: # read EOF in last iteration
                ret = self.buff
                self.buff = b""
                return ret
            data = self.fileobj.read(self.blocksize)
            if data:
                self.buff += self.decompressor.decompress(data)
            else:
                self.buff += self.decompressor.flush()
