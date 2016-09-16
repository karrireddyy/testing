import os
from pyPdf import PdfFileReader, PdfFileWriter
from tempfile import NamedTemporaryFile
from PythonMagick import Image
import sys

pdfname=sys.argv[1]
#os.system("rm /tmp/test2/*")
#os.system("mkdir /tmp/test2")
cmd="cp /opt/%s /tmp/test2/ocr.pdf"%(pdfname)

#os.system("cp /opt/corrected.html /tmp/test2/some_0.hocr")
os.system(cmd)

reader = PdfFileReader(open("/tmp/test2/ocr.pdf", "rb"))
for page_num in xrange(reader.getNumPages()):
    writer = PdfFileWriter()
    writer.addPage(reader.getPage(page_num))
    temp = NamedTemporaryFile(prefix=str(page_num), suffix=".pdf", delete=False)
    writer.write(temp)
    temp.close()

    im = Image()
    im.density("300") # DPI, for better quality
    im.read(temp.name)
    im.write("/tmp/test2/some_%d.jpg" % (page_num))

    os.remove(temp.name)
