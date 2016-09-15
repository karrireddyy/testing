import sys

extension1 = sys.argv[1]
original = "/tmp/test/some_%s.hocr"%extension1
corrected = "/tmp/test/correct_%s.html"%extension1

first = open("/app/append/first.txt").read()
open(corrected, "w").write(first + open(original).read())

last = open("/app/append/last.txt").read()
f = open(corrected, 'a')
f.write(last)
f.close()
