#!/bin/bash -e

if [ ! -d wikipedia-pagecounts-hours ]; then
  mkdir wikipedia-pagecounts-hours
fi
cd wikipedia-pagecounts-hours

yyyy=2014
MM=06
dd=19

for hh in 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23
do
  wget https://dumps.wikimedia.org/other/pagecounts-raw/${yyyy}/${yyyy}-${MM}/pagecounts-${yyyy}${MM}${dd}-${hh}0000.gz
  gunzip pagecounts-${yyyy}${MM}${dd}-${hh}0000.gz
done