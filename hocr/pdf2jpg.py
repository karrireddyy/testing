import os
from pyPdf import PdfFileReader, PdfFileWriter
from tempfile import NamedTemporaryFile
from PythonMagick import Image
import sys

os.system("rm -rf /tmp/test")
os.system("mkdir /tmp/test")
files1=open("/tmp/file.txt").read()
cmd="cp /opt/content_extraction/%s /tmp/test/ocr.pdf"%files1

os.system(cmd)

reader = PdfFileReader(open("/tmp/test/ocr.pdf", "rb"))
for page_num in xrange(reader.getNumPages()):
    writer = PdfFileWriter()
    writer.addPage(reader.getPage(page_num))
    temp = NamedTemporaryFile(prefix=str(page_num), suffix=".pdf", delete=False)
    writer.write(temp)
    temp.close()

    im = Image()
    im.density("300") # DPI, for better quality
    im.read(temp.name)
    im.write("/tmp/test/some_%d.jpg" % (page_num))

    os.remove(temp.name)
