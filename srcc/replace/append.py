import os

original = "/opt/Coriginal.html"
corrected = "/opt/corrected.html"

first = open("/app/replace/first.txt").read()
open(corrected, "w").write(first + open(original).read())

last = open("/app/replace/last.txt").read()
f = open(corrected, 'a')
f.write(last)
f.close()

os.system("rm /opt/Coriginal.html")
