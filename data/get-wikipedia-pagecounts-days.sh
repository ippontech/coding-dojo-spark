#!/bin/bash -e

if [ ! -d wikipedia-pagecounts-days ]; then
  mkdir wikipedia-pagecounts-days
fi
cd wikipedia-pagecounts-days

wget https://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-05/pagecounts-20140518-000000.gz
wget https://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-05/pagecounts-20140525-000000.gz
wget https://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-06/pagecounts-20140601-000000.gz
wget https://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-06/pagecounts-20140608-000000.gz
wget https://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-06/pagecounts-20140615-000000.gz
gunzip *.gz
