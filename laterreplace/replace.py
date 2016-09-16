import os
import subprocess
total_files = int(subprocess.Popen("ls /tmp/test2/ | grep .hocr | wc -l", shell=True, stdout=subprocess.PIPE).stdout.read())

for i in range(0,total_files):
    file_name = "/tmp/test2/some_%s.hocr"%(i)
    myhocr = open(file_name).read()

    for item in myhocr.split("</correction-data>"):
        if "<title>" in item:
	    corrected = item [ item.find("<title>")+len("<title>") : ]

    file1 = "/tmp/test2/Coriginal.hocr"
    f = open(file1,'w')
    f.write(corrected)
    f.close()

    lines = file('/tmp/test2/Coriginal.hocr', 'r').readlines() 
    del lines[-1] 
    file('/tmp/test2/Coriginal.hocr', 'w').writelines(lines)



# Append code !! appending positional values to the HOCR file !! 

    original = "/tmp/test2/Coriginal.hocr"
    corrected = "/tmp/test2/some_%s.hocr"%i

    first = open("/app/laterreplace/first.txt").read()
    open(corrected, "w").write(first + open(original).read())
	
    if '</html>' not in open(corrected).read():
        last = open("/app/laterreplace/last.txt").read()
    	f = open(corrected, 'a')
    	f.write(last)
    	f.close()

os.system("rm /tmp/test2/Coriginal.hocr")

