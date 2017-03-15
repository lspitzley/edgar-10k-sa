import argparse
import codecs
from collections import namedtuple
import csv
from glob import glob
from itertools import product
import logging
from multiprocessing import Queue
import os
import time

from bs4 import BeautifulSoup
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
import requests
from tqdm import tqdm


SEC_GOV_URL = 'http://www.sec.gov/Archives'
FORM_INDEX_URL = os.path.join(SEC_GOV_URL,'edgar','full-index','{}','QTR{}','form.idx')

IndexRecord = namedtuple("IndexRecord",["form_type","company_name","cik","date_filed","filename"])

class FormIndex(object):
    def __init__(self, index_dir):
        self.formrecords = []

        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)

    def retrieve(self, year, qtr):

        form_idx = "form_year{}_qtr{}.index".format(year,qtr)
        form_idx_path = os.path.join(self.index_dir,form_idx)

        self.download(form_idx_path, year, qtr)
        self.extract(form_idx_path)

    def download(self, form_idx_path, year, qtr):
        if os.path.exists(form_idx_path):
            print("Download skipped: year {}, qtr {}".format(year,qtr))
            return

        # Download and save to cached directory
        print("Downloading year {}, qtr {}".format(year,qtr))

        index_url = FORM_INDEX_URL.format(year,qtr)
        resp = requests.get(index_url)

        with open(form_idx_path,'wb') as fout:
            fout.write(resp.content)

    def extract(self, form_idx_path):
        # Parse row to record
        def parse_row_to_record(row, fields_begin):
            record = []

            for begin, end in zip(fields_begin[:],fields_begin[1:] + [len(row)] ):
                field = row[begin:end].rstrip()
                field = field.strip('\"')
                record.append(field)

            return record

        # Read and parse
        print("Extracting from {}".format(form_idx_path))

        with open(form_idx_path,'rb') as fin:
            # If arrived at 10-K section of forms
            arrived = False

            for row in fin.readlines():
                row = row.decode('ascii')
                if row.startswith("Form Type"):
                    fields_begin = [ row.find("Form Type"),
                                     row.find("Company Name"),
                                     row.find('CIK'),
                                     row.find('Date Filed'),
                                     row.find("File Name") ]

                elif row.startswith("10-K "):
                    arrived = True
                    rec = parse_row_to_record(row,fields_begin)
                    self.formrecords.append(IndexRecord(*rec))

                elif arrived == True:
                    break

    def save(self, path):
        print("Saving records to {}".format(path))

        with open(path,'w') as fout:
            writer = csv.writer(fout,delimiter=',',quotechar='\"',quoting=csv.QUOTE_ALL)
            for rec in self.formrecords:
                writer.writerow( tuple(rec) )

class Form(object):
    def __init__(self, form_dir):
        # Initialize cache directory
        self.form_dir = form_dir
        if not os.path.exists(form_dir):
            os.makedirs(form_dir)
        self.txt_dir = './txt'
        if not os.path.exists(self.txt_dir):
            os.makedirs(self.txt_dir)

    def download(self, form10k_savepath):

        def iter_path_generator(form10k_savepath):
            with open(form10k_savepath,'r') as fin:
                reader = csv.reader(fin,delimiter=',',quotechar='\"',quoting=csv.QUOTE_ALL)
                for row in reader:
                    form_type, company_name, cik, date_filed, filename = row
                    url = os.path.join(SEC_GOV_URL,filename)
                    yield url

        def download_job(url):
            fname = '_'.join(url.split('/')[-2:])

            fname, ext = os.path.splitext(fname)
            htmlname = fname + '.html'

            formpath = os.path.join(self.form_dir,htmlname)
            text_path = os.path.join(self.txt_dir,fname + '.txt')

            if os.path.exists(text_path):
                print("Already exists, skipping {}".format(url))
            else:
                print("Downloading & Parsing {}".format(url))

                r = requests.get(url)
                try:
                    soup = BeautifulSoup( r.content, "lxml")
                    text = soup.get_text("\n").strip('\n')

                    """
                        Preprocess Text
                    """
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
                    """
                        End preprocess text
                    """
                    # Write to file
                    text_path = os.path.join(self.txt_dir,fname + '.txt')
                    with codecs.open(text_path,'w',encoding='utf-8') as fout:
                        fout.write(text)
                except:
                    print("Beautiful Soup Parsing failed for {}".format(url))

        ncpus = cpu_count() if cpu_count() <= 8 else 8;
        pool = ProcessPool( ncpus )
        pool.map( download_job,
                    iter_path_generator(form10k_savepath) )

class MDAParser(object):
    def __init__(self, mda_dir, txt_dir):
        self.mda_dir    = mda_dir
        if not os.path.exists(mda_dir):
            os.makedirs(mda_dir)

        self.txt_dir    = txt_dir
        if not os.path.exists(txt_dir):
            os.makedirs(txt_dir)

    def extract(self):

        def text_gen(txt_dir):
            # Yields markup & name
            for fname in os.listdir(txt_dir):
                if not fname.endswith('.txt'):
                    continue
                # Read html
                print("Parsing: {}".format(fname))
                filepath = os.path.join(txt_dir,fname)
                with codecs.open(filepath,'rb',encoding='utf-8') as fin:
                    text = fin.read()

                name, ext = os.path.splitext(fname)

                yield text, name

        def parsing_job(params):
            text, name = params
            msg = ""
            mda, end = self.parse_mda(text)
            # Parse second time if first parse results in index
            if mda and len(mda.encode('utf-8')) < 1000:
                mda, _ = self.parse_mda(text, start=end)

            if mda: # Has value
                msg = "SUCCESS"
                mda_path = os.path.join(self.mda_dir, name + '.mda')
                with codecs.open(mda_path,'w', encoding='utf-8') as fout:
                    fout.write(mda)
            else:
                msg = msg if mda else "MDA NOT FOUND"
            print("{},{}".format(name,msg))
            return name + '.txt', msg #


        ncpus = cpu_count() if cpu_count() <= 8 else 8
        pool = ProcessPool( ncpus )

        _start = time.time()
        parsing_failed = pool.map( parsing_job, \
                                   text_gen(self.txt_dir) )
        _end = time.time()

        print("MDA parsing time taken: {} seconds.".format(_end-_start))

        # Write failed parsing list
        count = 0
        parsing_log = 'parsing.log'
        with open(parsing_log,'w') as fout:
            print("Writing parsing results to {}".format(parsing_log))
            for name, msg in parsing_failed:
                fout.write('{},{}\n'.format(name,msg))
                if msg != "SUCCESS":
                    count = count + 1

        print("Number of failed text:{}".format(count))

    def parse_mda(self, text, start=0):
        debug = False
        """
            Return Values
        """

        mda = ""
        end = 0

        """
            Parsing Rules
        """

        # Define start & end signal for parsing
        item7_begins = [ '\nITEM 7.', '\nITEM 7 –','\nITEM 7:', '\nITEM 7 ', '\nITEM 7\n' ]
        item7_ends   = [ '\nITEM 7A' ]
        if start != 0:
            item7_ends.append('\nITEM 7') # Case: ITEM 7A does not exist
        item8_begins = [ '\nITEM 8'  ]

        """
            Parsing code section
        """
        text = text[start:]

        # Get begin
        for item7 in item7_begins:
            begin = text.find(item7)
            if debug:
                print(item7,begin)
            if begin != -1:
                break

        if begin != -1: # Begin found
            for item7A in item7_ends:
                end = text.find(item7A, begin+1)
                if debug:
                    print(item7A,end)
                if end != -1:
                    break

            if end == -1: # ITEM 7A does not exist
                for item8 in item8_begins:
                    end = text.find(item8, begin+1)
                    if debug:
                        print(item8,end)
                    if end != -1:
                        break

            # Get MDA
            if end > begin:
                mda = text[begin:end].strip()
            else:
                end = 0

        return mda, end


def main():
    # Download form index
    parser = argparse.ArgumentParser("Edgar 10k forms sentiment analysis")
    parser.add_argument('--year_start',type=int,default=2014)
    parser.add_argument('--year_end',  type=int,default=2016)
    parser.add_argument('--index_dir',type=str,default='./index')
    parser.add_argument('--form_dir',type=str,default='./form')
    parser.add_argument('--txt_dir',type=str,default='./txt')
    parser.add_argument('--mda_dir',type=str,default='./mda')
    args = parser.parse_args()

    year_start = args.year_start
    year_end = args.year_end

    index_dir = args.index_dir
    form_dir  = args.form_dir
    txt_dir   = args.txt_dir
    mda_dir   = args.mda_dir

    # Download and extract 10k form indices
    form10k_savepath = "year{}-{}.10k.csv".format(year_start,year_end)

    if not os.path.exists(form10k_savepath):
        formindex = FormIndex(index_dir=index_dir)
        for year, qtr in product(range(args.year_start,args.year_end+1),range(1,5)):
            formindex.retrieve(year, qtr)
        formindex.save(form10k_savepath)
    else:
        print("{} already exists".format(form10k_savepath))

    # Download 10k forms raw data
    form = Form(form_dir=form_dir)
    form.download(form10k_savepath=form10k_savepath)

    # Extract MD&A
    parser = MDAParser(mda_dir=mda_dir,
                       txt_dir=txt_dir)
    parser.extract()

if __name__ == "__main__":
    main()
