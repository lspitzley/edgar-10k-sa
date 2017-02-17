import argparse
import csv
from collections import namedtuple
from multiprocessing import Process, Queue
import os
import requests

from tqdm import tqdm

SEC_GOV_URL = 'http://www.sec.gov/Archives/'
FORM_INDEX_URL = os.path.join(SEC_GOV_URL,'edgar','full-index','{}','QTR{}','form.idx')

IndexRecord = namedtuple("IndexRecord",["form_type","company_name","cik","date_filed","filename"])

class FormIndex(object):
    def __init__(self):
        self.formrecords = []

    def download(self, year):
        # Parse row to record
        def parse_row_to_record(row, fields_begin):
            if len(fields_begin) == 0:
                raise ValueError

            record = []

            for cbegin, cend in zip(fields_begin[:],fields_begin[1:] + [len(row)] ):
                field = row[cbegin:cend].rstrip()
                record.append(field)

            return record

        for qtr in range(1,2):

            index_url = FORM_INDEX_URL.format(year,qtr)
            print("Downloading from {}".format(index_url))

            resp = requests.get(index_url)

            lines = resp.text.split('\n')


            fields_begin = []

            for row in lines:
                if row.startswith("Form Type"):
                    fields_begin = [ row.find("Form Type"),
                                     row.find("Company Name"),
                                     row.find('CIK'),
                                     row.find('Date Filed'),
                                     row.find("File Name") ]

                elif row.startswith("10-K"):
                    rec = parse_row_to_record(row,fields_begin)
                    self.formrecords.append(IndexRecord(*rec))

        #import pdb; pdb.set_trace()

    def save(self, path):
        print("Saving records to {}".format(path))

        with open(path,'w') as fout:
            writer = csv.writer(fout)
            for rec in self.formrecords:
                writer.writerow( tuple(rec) )

def main():
    # Download form index
    formindex = FormIndex()
    path = 'form2006.index'
    for year in range(2006,2007):
        formindex.download(year)
        formindex.save(path)

if __name__ == "__main__":
    main()
