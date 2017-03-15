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

"""
    ncpus = cpu_count() if cpu_count() <= 8 else 8;
    pool = ProcessPool( ncpus )
    pool.map( download_job,
                iter_path_generator(form10k_savepath) )
"""

def preprocess_job(txt_path):

    print("Preprocessing {}".format(txt_path))
    with codecs.open(txt_path,'r',encoding='utf-8') as fin:
        text = fin.read()

    # Preprocess text here
    text = text.upper() # Convert to upper
    text = '\n'.join(text.splitlines())
    text = text.replace('\n.\n','.\n')

    text = re.sub(r'[\n]+',r'\n',text)

    # Write new file
    txt_name = os.path.basename(txt_path)
    new_path = os.path.join(tar_dir,txt_name)
    with codecs.open(new_path,'w',encoding='utf-8') as fout:
        fout.write(text)


def mlp():

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
        text = unicodedata.normalize("NFKD", text) # Normalize
        text = text.upper() # Convert to upper
        text = '\n'.join(text.splitlines()) # Let python take care of unicode break lines

        text = re.sub(r'[ ]+\n', '\n', text) # Take take of Whitespaces and breaklines
        text = re.sub(r'\n[ ]+', '\n', text)
        text = re.sub(r'\n+', '\n', text)

        text = text.replace('\n.\n','.\n') # Move Period to beginning
        text = text.replace('$\n','$')

        text = text.replace('\n','\n\n') # Reformat by additional breakline


        import pdb; pdb.set_trace()
        # Write new file
        txt_name = os.path.basename(txt_path)
        new_path = os.path.join(tar_dir,txt_name)
        with codecs.open(new_path,'w',encoding='utf-8') as fout:
            fout.write(text)

if __name__ == "__main__":
    main()
