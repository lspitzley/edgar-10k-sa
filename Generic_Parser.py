"""
Program to provide generic parsing for all files in user-specified directory.
The program assumes the input files have been scrubbed,
  i.e., HTML, ASCII-encoded binary, and any other embedded document structures that are not
  intended to be analyzed have been deleted from the file.

Dependencies:
    Python:  Load_MasterDictionary.py
    Data:    LoughranMcDonald_MasterDictionary_2014.csv

The program outputs:
   1.  File name
   2.  File size (in bytes)
   3.  Number of words (based on LM_MasterDictionary
   4.  Proportion of positive words (use with care - see LM, JAR 2016)
   5.  Proportion of negative words
   6.  Proportion of uncertainty words
   7.  Proportion of litigious words
   8.  Proportion of modal-weak words
   9.  Proportion of modal-moderate words
  10.  Proportion of modal-strong words
  11.  Proportion of constraining words (see Bodnaruk, Loughran and McDonald, JFQA 2015)
  12.  Number of alphanumeric characters (a-z, A-Z, 0-9)
  13.  Number of alphabetic characters (a-z, A-Z)
  14.  Number of digits (0-9)
  15.  Number of numbers (collections of digits)
  16.  Average number of syllables
  17.  Averageg word length
  18.  Vocabulary (see Loughran-McDonald, JF, 2015)

  ND-SRAF
  McDonald 2016/06
"""

import csv
import glob
import os
import re
import string
import sys
import time
#sys.path.append('D:\GD\Python\TextualAnalysis\Modules')  # Modify to identify path for custom modules
import Load_MasterDictionary as LM

from tqdm import tqdm

"""
    Specify File Locations for Generic Parser.py
"""

# User defined directory for files to be parsed
TARGET_FILES = r'./mda/*.mda'

# User defined file pointer to LM dictionary
MASTER_DICTIONARY_FILE = r'./LoughranMcDonald_MasterDictionary_2014.csv'

# User defined output file
OUTPUT_FILE = r'./result.csv'

# Setup output
OUTPUT_FIELDS = ['filename', 'file size', 'number of words', '% positive', '% negative',
                 '% uncertainty', '% litigious', '% modal-weak', '% modal moderate',
                 '% modal strong', '% constraining', '# of alphanumeric', '# of digits',
                 '# of numbers', 'avg # of syllables per word', 'average word length', 'vocabulary',
                 'CIK']

lm_dictionary = LM.load_masterdictionary(MASTER_DICTIONARY_FILE, True)

def main():

    f_out = open(OUTPUT_FILE, 'w')
    wr = csv.writer(f_out, lineterminator='\n')
    wr.writerow(OUTPUT_FIELDS)

    file_list = glob.glob(TARGET_FILES)

    for filename in tqdm(file_list):
        with open(filename, 'r', encoding='UTF-8', errors='ignore') as f_in:
            doc = f_in.read()
        doc_len = len(doc)
        doc = re.sub('(May|MAY)', ' ', doc)  # drop all May month references
        doc = doc.upper()  # for this parse caps aren't informative so shift

        output_data = get_data(doc)

        fname = os.path.basename(filename)

        CIK = fname.split('_')[0]
        """
            Leave only basic filename for joining meta information
        """
        #output_data[0] = filename
        clean_filename  = filename.split('/')[-1].rstrip('.mda')
        output_data[0]  = clean_filename
        output_data[1]  = doc_len
        output_data[-1] = CIK
        wr.writerow(output_data)


def get_data(doc):

    vdictionary = {}
    _odata = [0] * 18 # Modified for CIK
    total_syllables = 0
    word_length = 0

    tokens = re.findall('\w+', doc)  # Note that \w+ splits hyphenated words
    for token in tokens:
        if not token.isdigit() and len(token) > 1 and token in lm_dictionary:
            _odata[2] += 1  # word count
            word_length += len(token)
            if token not in vdictionary:
                vdictionary[token] = 1
            if lm_dictionary[token].positive: _odata[3] += 1
            if lm_dictionary[token].negative: _odata[4] += 1
            if lm_dictionary[token].uncertainty: _odata[5] += 1
            if lm_dictionary[token].litigious: _odata[6] += 1
            if lm_dictionary[token].weak_modal: _odata[7] += 1
            if lm_dictionary[token].moderate_modal: _odata[8] += 1
            if lm_dictionary[token].strong_modal: _odata[9] += 1
            if lm_dictionary[token].constraining: _odata[10] += 1
            total_syllables += lm_dictionary[token].syllables

    _odata[11] = len(re.findall('[A-Z]', doc))
    _odata[12] = len(re.findall('[0-9]', doc))
    # drop punctuation within numbers for number count
    doc = re.sub('(?!=[0-9])(\.|,)(?=[0-9])', '', doc)
    doc = doc.translate(str.maketrans(string.punctuation, " " * len(string.punctuation)))
    _odata[13] = len(re.findall(r'\b[-+\(]?[$€£]?[-+(]?\d+\)?\b', doc))
    _odata[14] = total_syllables / _odata[2]
    _odata[15] = word_length / _odata[2]
    _odata[16] = len(vdictionary)

    # Convert counts to %
    for i in range(3, 10 + 1):
        _odata[i] = (_odata[i] / _odata[2]) * 100
    # Vocabulary

    return _odata


if __name__ == '__main__':
    print('\n' + time.strftime('%c') + '\nGeneric_Parser.py\n')
    main()
    print('\n' + time.strftime('%c') + '\nNormal termination.')
