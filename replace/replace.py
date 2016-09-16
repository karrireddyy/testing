import os


myhtml = open("/opt/correction.html").read()

for item in myhtml.split("</correction-data>"):
	if "<title>" in item:
		corrected = item [ item.find("<title>")+len("<title>") : ]

file1 = "/opt/Coriginal.html"
f = open(file1,'w')
f.write(corrected)
f.close()

lines = file('/opt/Coriginal.html', 'r').readlines() 
del lines[-1] 
file('/opt/Coriginal.html', 'w').writelines(lines)

os.system("python /app/replace/append.py")
