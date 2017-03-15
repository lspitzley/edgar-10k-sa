import codecs
from glob import glob
import os

from tqdm import tqdm

src_dir = './txt'
tar_dir = './ptxt'


def main():

    if not os.path.exists(tar_dir):
        os.makedirs(tar_dir)


    for txt_path in tqdm(glob(os.path.join(src_dir,'*.txt'))):
        # Read old file
        with codecs.open(txt_path,'r',encoding='utf-8') as fin:
            text = fin.read()

        # Preprocess text here
        text = text.upper() # Convert to upper
        text = '\n'.join(text.split('\n'))
        text = text.replace('\n.\n','.\n')

        # Write new file
        txt_name = os.path.basename(txt_path)
        new_path = os.path.join(tar_dir,txt_name)
        with codecs.open(new_path,'w',encoding='utf-8') as fout:
            fout.write(text)

if __name__ == "__main__":
    main()
