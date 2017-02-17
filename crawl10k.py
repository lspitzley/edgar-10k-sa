"""
    Alter parameters with argparse
"""

import argparse
from multiprocessing import Process, Queue
import os

def download_forms(form_url):
    pass

def main():
    parser = argparse.ArgumentParser('Edgar 10k filings sentiment analysis')
    parser.add_argument('--year-begin',type=int,default=1993)
    parser.add_argument('--year-end',type=int,default=2016)
    parser.add_argument('--cache-path',type=str,default='./cache/')
    args = parser.parse_args()

    print(vars(args))

if __name__ == "__main__":
    main()
