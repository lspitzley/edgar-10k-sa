import csv
from glob import glob
import os
import sys

from tqdm import tqdm

from encoder import Model

if __name__ == "__main__":

    model = Model()
    mda_dir = './mda'
    outfile = 'gen_review_feature.csv'

    batch_size = 1024
    mda_files = glob(os.path.join(mda_dir,'*.mda'))
    # Filter 2010-2015 files    
    #mda_files = list(filter(lambda x: int(x.split('-')[1]) in range(10,16), mda_files))
    
    def iter_batch(lines, batch_size=256):
        # Filter empty lines
        lines = list(filter(lambda x: x.strip(), lines))
        for idx in range(0, len(lines), batch_size):
            yield lines[idx:idx+batch_size]


    with open(outfile,'w') as fout:
        writer = csv.writer(fout,delimiter=',')
        writer.writerow(['mda', 'paragraph'] + list(range(4096)))
        for mda_file in tqdm(mda_files):

            with open(mda_file,'r',encoding='utf-8') as fin:
                text = fin.read()

            count = 0
            for lines in tqdm(list(iter_batch(text.split('\n\n'), batch_size))):

                features = model.transform(lines)
                
                list_feature = features.tolist()
                for feat in list_feature:
                    row = [ mda_file, count ] + feat
                    writer.writerow(row)
                    count += 1
