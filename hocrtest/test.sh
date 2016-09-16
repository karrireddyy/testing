python /app/hocrtest/pdf2jpg.py $1

python /app/hocrtest/jpg2hocr.py

/app/hocrtest/hocr-pdf  /tmp/test/ . > "/tmp/test/some_0.pdf"
