import argparse
import csv
from collections import namedtuple
from itertools import product
import os
import requests

from tqdm import tqdm

SEC_GOV_URL = 'http://www.sec.gov/Archives'
FORM_INDEX_URL = os.path.join(SEC_GOV_URL,'edgar','full-index','{}','QTR{}','form.idx')

IndexRecord = namedtuple("IndexRecord",["form_type","company_name","cik","date_filed","filename"])

class FormIndex(object):
    def __init__(self):
        self.formrecords = []
        # Initialize cache directory
        if not os.path.exists('./cache'):
            os.makedirs("./cache/")

    def retrieve(self, year, qtr):

        form_idx = "form_year{}_qtr{}.index".format(year,qtr)
        form_idx_path = os.path.join('.','cache',form_idx)

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
            writer = csv.writer(fout)
            for rec in self.formrecords:
                writer.writerow( tuple(rec) )

class Form(object):
    def __init__(self):
        pass

    def download(self, form10k_savepath):

        with open(form10k_savepath,'r') as fin:
            reader = csv.reader(fin, delimiter=',')
            for row in reader:
                form_type, company_name, cik, date_filed, filename = row

                url = os.path.join(SEC_GOV_URL,filename)

                r = requests.get(url)




def main():
    # Download form index
    parser = argparse.ArgumentParser("Edgar 10k forms sentiment analysis")
    parser.add_argument('--year_start',type=int,default=1993)
    parser.add_argument('--year_end',type=int,default=2016)
    args = parser.parse_args()
    # Weird: 2011, QTR4
    year_start = args.year_start
    year_end = args.year_end

    # Download and extract 10k forms
    form10k_savepath = "year{}-{}.10k.csv".format(year_start,year_end)
    if not os.path.exists(form10k_savepath):
        formindex = FormIndex()
        for year, qtr in product(range(args.year_start,args.year_end+1),range(1,5)):
            formindex.retrieve(year, qtr)
        formindex.save(form10k_savepath)
    else:
        print("{} already exists".format(form10k_savepath))


    # Download 10k forms and extract MD&A
    try:
        os.makedirs('./mda')
    except:
        pass

    form = Form()



if __name__ == "__main__":
    main()
