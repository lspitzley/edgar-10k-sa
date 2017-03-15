import os

failedfile = 'failed2parse.txt'

src_dir = './txt'
tar_dir = './ftxt'

if not os.path.exists(tar_dir):
    os.makedirs(tar_dir)


with open(failedfile,'r') as fin:
    for line in fin.readlines():
        line = line.strip('\n')

        text_path = os.path.join(src_dir,line)

        os.system('mv {} {}'.format(text_path,tar_dir))
