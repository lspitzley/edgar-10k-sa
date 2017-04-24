import csv
from glob import glob
import os
import sys

from tqdm import tqdm

from encoder import Model

model = Model()

mda_dir = './1996_2013mda'
outfile = 'gen_review_feature_paragraph_1993-2013.csv'

batch_size = 1024
mda_files = glob(os.path.join(mda_dir,'*.mda'))
# Filter 2010-2015 files    

def iter_file(mda_files, batch_size=256):
    for idx in range(0,len(mda_files),batch_size):
        text_list = []
        for b in range(batch_size):
            if idx + b == len(mda_files):
                break
            with open(mda_files[idx+b],'r') as fin:
                text = fin.read()
            text_list.append(text)
        yield mda_files[idx:idx+batch_size], text_list

with open(outfile,'w') as fout:
    writer = csv.writer(fout,delimiter=',')
    writer.writerow(['mda','2388'])
    for mda_names, text_list in tqdm(iter_file(mda_files,batch_size)):

        features = model.transform(text_list)
        list_feature = features[:,2388].tolist()
        for mda_name, feat in zip(mda_names, list_feature):
            row = [ mda_name, feat ] 
            writer.writerow(row)
