import csv
from glob import glob
import os

from tqdm import tqdm

from encoder import Model

if __name__ == "__main__":

    model = Model()
    mda_dir = './mda'
    outfile = 'gen_review_feature.csv'

    with open(outfile,'w') as fout:
        writer = csv.writer(fout,delimiter=',')
        writer.writerow(['mda'] + list(range(4096)))
        for mda_file in tqdm(glob(os.path.join(mda_dir,'*.mda'))):

            with open(mda_file,'r',encoding='utf-8') as fin:
                text = fin.read()

            feature = model.transform([text])

            row = [ mda_file ] + feature.tolist()[0]

            writer.writerow(row)
