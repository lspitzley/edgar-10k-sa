import argparse
from glob import glob
import os
import re

from tqdm import tqdm

from encoder import Model

def iter_file(mda_files, batch_size, paragraph=False):
    if not paragraph:
        for idx in range(0, len(mda_files), batch_size):
            text_list = []
            for b in range(batch_size):
                if idx + b == len(mda_files):
                    break
                with open(mda_files[idx + b], 'r') as fin:
                    text = fin.read()
                text_list.append(text)
            yield mda_files[idx:idx + batch_size], text_list
    else:
        for filename in mda_files:
            with open(filename, 'r') as fin:
                text = fin.read()

            lines = text.splitlines()
            lines = [ x for x in lines if re.search('[A-Z]', x) ]

            for idx in range(0, len(lines), batch_size):
                begin = idx
                end = idx + batch_size if idx + \
                    batch_size < len(lines) else len(lines) - 1
                yield [filename] * (end-begin), list(range(begin, end)), lines[begin:end]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--mda_dir', type=str, help="mda file directory")
    parser.add_argument('-p', '--paragraph', action="store_true",
                        help="encode by paragraph, else by document")
    parser.add_argument('-b', '--batch_size', type=int,
                        default=1024, help='batch size of model')
    parser.add_argument('-o', '--out_file', type=str, help='out file name')
    args = parser.parse_args()

    mda_files = glob(os.path.join(args.mda_dir, '*.mda'))
    text_iterator = iter_file(mda_files, args.batch_size, args.paragraph)
    header = ['mda_file'] + \
        (['paragraph'] if args.paragraph else []) + ['2388']

    model = Model()

    with open(args.out_file, 'w') as fout:
        fout.write(','.join(header) + '\n')
        for items in tqdm(text_iterator):
            try:
                text_list = items[-1]
                
                features = model.transform(text_list)
                
                list_feature = features[:, 2388].tolist()
                
                items = items[:-1] + tuple([list_feature])

                for row in zip(*items):
                    fout.write(','.join(list(map(lambda x: str(x), row))) + '\n')
            except:
                print("Skipping {}...".format(items[0][0]))
