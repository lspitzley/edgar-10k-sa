import argparse
from itertools import product
import os

from formindex import FormIndex
from form10k import Form10k
from mdaparser import MDAParser

def main():
    ###########################
    #        Arguments        #
    ###########################
    parser = argparse.ArgumentParser("Edgar 10k forms sentiment analysis")
    parser.add_argument('--year_start',type=int,default=2014)
    parser.add_argument('--year_end',type=int,default=2016)
    parser.add_argument('--index_dir',type=str,default='./index')
    parser.add_argument('--txt_dir',type=str,default='./txt')
    parser.add_argument('--mda_dir',type=str,default='./mda')
    args = parser.parse_args()

    year_start = args.year_start
    year_end = args.year_end

    index_dir = args.index_dir
    txt_dir   = args.txt_dir
    mda_dir   = args.mda_dir

    # Download indices and cache to index directory
    index_path = "year{}-{}.10k.csv".format(year_start,year_end)
    
    formindex = FormIndex(index_dir=index_dir)
    if not os.path.exists(index_path):
        formindex = FormIndex(index_dir=index_dir)
        for year, qtr in product(range(args.year_start,args.year_end+1),range(1,5)):
            formindex.retrieve(year, qtr)
        formindex.save(index_path)
    else:
        print("{} already exists".format(index_path))

    # Download 10k forms, parse html and preprocess text
    form10k = Form10k(txt_dir=txt_dir)
    form10k.download(index_path=index_path)

    # Extract MD&A from processed text
    # Note that the parser parses every text in the txt_dir, not according to the index file
    parser = MDAParser(txt_dir=txt_dir, mda_dir=mda_dir)
    parser.extract()

if __name__ == "__main__":
    main()
