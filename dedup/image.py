import io
import struct

import PIL.Image

class ImageHash(object):
    """A hash on the contents of an image. This disregards mode, depth and meta
    information. Note that due to limitations in PIL and the image format
    (interlacing) the full contents are stored and decoded in hexdigest."""
    maxsize = 1024 * 1024 * 32
    # max memory usage is about 5 * maxpixels in bytes
    maxpixels = 1024 * 1024 * 32

    def __init__(self, hashobj):
        """
        @param hashobj: a hashlib-like object
        """
        self.hashobj = hashobj
        self.imagedetected = False
        self.content = io.BytesIO()

    def update(self, data):
        self.content.write(data)
        if self.content.tell() > self.maxsize:
            raise ValueError("maximum image size exceeded")
        if self.imagedetected:
            return
        if self.content.tell() < 33: # header + IHDR
            return
        curvalue = self.content.getvalue()
        if curvalue.startswith(b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR"):
            width, height = struct.unpack(">II", curvalue[16:24])
            if width * height > self.maxpixels:
                raise ValueError("maximum image pixels exceeded")
            self.imagedetected = True
            return
        raise ValueError("not a png image")

    def copy(self):
        new = ImageHash()
        new.hashobj = self.hashobj.copy()
        new.imagedetected = self.imagedetected
        new.content = io.BytesIO(self.content.getvalue())
        return new

    def hexdigest(self):
        if not self.imagedetected:
            raise ValueError("not a png image")
        hashobj = self.hashobj.copy()
        pos = self.content.tell()
        try:
            self.content.seek(0)
            try:
                img = PIL.Image.open(self.content)
            except IOError:
                raise ValueError("broken png header")
            width, height = img.size
            pack = lambda elem: struct.pack("BBBB", *elem)
            # special casing easy modes reduces memory usage
            if img.mode == "L":
                pack = lambda elem: struct.pack("BBBB", elem, elem, elem, 255)
            elif img.mode == "RGB":
                pack = lambda elem: struct.pack("BBBB", *(elem + (255,)))
            elif img.mode != "RGBA":
                try:
                    img = img.convert("RGBA")
                except (SyntaxError, IndexError, IOError): # crazy stuff from PIL
                    raise ValueError("error reading png image")
            try:
                for elem in img.getdata():
                    hashobj.update(pack(elem))
            except (SyntaxError, IndexError, IOError): # crazy stuff from PIL
                raise ValueError("error reading png image")
        finally:
            self.content.seek(pos)
        return "%s%8.8x%8.8x" % (hashobj.hexdigest(), width, height)
