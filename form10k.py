import codecs
import csv
from glob import glob
import os
import re
import sys
import unicodedata

from bs4 import BeautifulSoup
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
import requests

SEC_GOV_URL = 'https://www.sec.gov/Archives'

class Form10k(object):
    def __init__(self, txt_dir):
        # Save to txt dir
        self.txt_dir = './txt'
        if not os.path.exists(self.txt_dir):
            os.makedirs(self.txt_dir)

    def _process_text(self, text):
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

        return text


    def download(self, index_path):

        def iter_path_generator(index_path):
            with open(index_path,'r') as fin:
                reader = csv.reader(fin,delimiter=',',quotechar='\"',quoting=csv.QUOTE_ALL)
                for row in reader:
                    form_type, company_name, cik, date_filed, filename = row
                    url = os.path.join(SEC_GOV_URL,filename).replace("\\","/")
                    yield url

        def download_job(url):
            fname = '_'.join(url.split('/')[-2:])

            fname, ext = os.path.splitext(fname)
            htmlname = fname + '.html'

            text_path = os.path.join(self.txt_dir,fname + '.txt')

            if os.path.exists(text_path):
                print("Already exists, skipping {}".format(url))
            else:
                print("Downloading & Parsing {}".format(url))

                r = requests.get(url)
                try:
                    # Parse html with Beautiful Soup
                    soup = BeautifulSoup( r.content, "html.parser" )
                    text = soup.get_text("\n")

                    # Process Text
                    text = self._process_text(text)

                    text_path = os.path.join(self.txt_dir,fname + '.txt')
                    # Write to file
                    with codecs.open(text_path,'w',encoding='utf-8') as fout:
                        fout.write(text)
                except BaseException as e:
                    print("{} parsing failed: {}".format(url,e))


        ncpus = cpu_count() if cpu_count() <= 8 else 8;
        pool = ProcessPool( ncpus )
        pool.map( download_job,
                    iter_path_generator(index_path) )
