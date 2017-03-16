import codecs
import csv
import os
import time

from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
import requests

class MDAParser(object):
    def __init__(self, txt_dir, mda_dir):
        self.txt_dir    = txt_dir
        if not os.path.exists(txt_dir):
            os.makedirs(txt_dir)

        self.mda_dir    = mda_dir
        if not os.path.exists(mda_dir):
            os.makedirs(mda_dir)

    def extract(self):

        def text_gen(txt_dir):
            # Yields markup & name
            for fname in os.listdir(txt_dir):
                if not fname.endswith('.txt'):
                    continue
                yield fname

        def parsing_job(fname):
            print("Parsing: {}".format(fname))
            # Read text
            filepath = os.path.join(self.txt_dir,fname)
            with codecs.open(filepath,'rb',encoding='utf-8') as fin:
                text = fin.read()

            name, ext = os.path.splitext(fname)
            # Parse MDA part

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
