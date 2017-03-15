import codecs
from glob import glob
import os
import re

from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
from tqdm import tqdm
import unicodedata

src_dir = './txt'
tar_dir = './ptxt'

def process_text(text):
    # Normalize due to unicode
    text = unicodedata.normalize("NFKD", text) # Normalize
    text = '\n'.join(text.splitlines()) # Let python take care of unicode break lines

    # Convert to upper
    text = text.upper() # Convert to upper

    # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n','.\n') # Move Period to beginning

    text = text.replace('\nI\nTEM','\nITEM')
    text = text.replace('\nITEM\n','\nITEM ')
    text = text.replace('\nITEM  ','\nITEM ')

    text = text.replace(':\n','.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n','$')
    text = text.replace('\n%','%')

    # Reformat
    text = text.replace('\n','\n\n') # Reformat by additional breakline

    return text

def preprocess_job(txt_path):
    txt_name = os.path.basename(txt_path)
    new_path = os.path.join(tar_dir,txt_name)
    if os.path.exists(new_path):
        print("{} already exists, skipping".format(new_path))
        return

    print("Preprocessing {}".format(txt_path))
    with codecs.open(txt_path,'r',encoding='utf-8') as fin:
        text = fin.read()

    # Preprocess text here
    text = process_text(text)

    # Write new files
    with codecs.open(new_path,'w',encoding='utf-8') as fout:
        fout.write(text)

def mlp():
    if not os.path.exists(tar_dir):
        os.makedirs(tar_dir)

    iterator = glob(os.path.join(src_dir,'*.txt'))

    ncpus = cpu_count() if cpu_count() <= 8 else 8;
    pool = ProcessPool( ncpus )
    pool.map( preprocess_job, iterator )


def main():

    if not os.path.exists(tar_dir):
        os.makedirs(tar_dir)

    for txt_path in tqdm(glob(os.path.join(src_dir,'*.txt'))):
        # Read old file
        with codecs.open(txt_path,'r',encoding='utf-8') as fin:
            text = fin.read()

        # Preprocess text here
        text = process_text(text)

        # Write new file
        txt_name = os.path.basename(txt_path)
        new_path = os.path.join(tar_dir,txt_name)
        with codecs.open(new_path,'w',encoding='utf-8') as fout:
            fout.write(text)

if __name__ == "__main__":
    #main()
    mlp()
