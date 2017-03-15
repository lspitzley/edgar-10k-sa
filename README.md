# edgar-10k-sa

This repo downloads edgar form 10k and then try and extract the mda section

There are 3 parts:
  1. Class FormIndex:
    - First we download the full indexes of year range
    - Save to csv file

  2. Class Form:
    - We download with http requests(edgar closed ftp service since 2017) with index csv file
    - Use BeautifulSoup to parse the raw html and preprocess text for easier parsing
    - Save to txt dir

  3. Class MDAParser:
    - Try to extract MDA section from preprocessed text
    - Save file to mda dir
    - Save parsing results to parsing.log
