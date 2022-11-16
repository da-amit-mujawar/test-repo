import subprocess

def fetch_header(bucket,file,suffix):
    cat_val = 'cat'
    if "gz" in suffix:
        print("gz compression found")
        cat_val = 'zcat'
    filename = "s3://"+bucket.name+"/"+file.key
    print("file name is : ",filename )
    cmd = "aws s3 cp "+filename+" - | "+cat_val+" | head -n 1"
    print("cmd is : ",cmd)
    ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    header_string = ps.communicate()[0].decode("utf-8").split("\n",1)[0].strip()
    print('header for file: ', file.key,' is ',header_string )
    return header_string

