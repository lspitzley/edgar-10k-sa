import argparse
from glob import glob
import os

import numpy as np
from tqdm import tqdm

from encoder import Model

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--mda_dir', type=str, help="mda file directory")
    parser.add_argument('-b', '--batch_size', type=int,
                        default=1024, help='batch size of model')
    parser.add_argument('-o', '--out_file', type=str, help='out file name')
    args = parser.parse_args()

    mda_files = glob(os.path.join(args.mda_dir, '*.mda'))
    batch_size = args.batch_size
    out_file = args.out_file

    header = ['mda_file','mean','std','25%','50%','75%']
    fout = open(out_file,'w')
    fout.write(','.join(header)+'\n')


    model = Model()

    for mda_file in tqdm(mda_files):
        
        with open(mda_file,'r') as fin:
            text = fin.read()
        
        lines = list(filter(lambda x: x.strip(), text.splitlines()))

        feature_list = []

        for idx in tqdm(range(0, len(lines), batch_size)):
            line_list = lines[idx:idx+batch_size]

            features = model.transform(line_list)

            feature_list += features[:,2388].tolist()
        
        mean = np.mean(feature_list)
        std = np.std(feature_list)

        quartile1 = np.percentile(feature_list, 25)
        median = np.median(feature_list)
        quartile3 = np.percentile(feature_list, 75)

        row = list(map(str,[ mda_file, mean, std, quartile1, median, quartile3 ]))

        fout.write(','.join(row)+'\n')

    fout.close()
