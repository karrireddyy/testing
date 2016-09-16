import os
import subprocess

from pytesseract import pytesseract

test=int(subprocess.Popen("ls /tmp/test/ | grep .jpg | wc -l", shell=True, stdout=subprocess.PIPE).stdout.read())

for i in range(0,test):
	vjpg = "/tmp/test/some_%d.jpg"%i
	vosc = "cp output.hocr /tmp/test/some_%d.hocr"%i
	pytesseract.run_tesseract(vjpg, 'output', lang=None, boxes=False, config="hocr")
	os.system(vosc)