#!/bin/bash

dir=../dojo-spark/src/main/resources/data/insee

count=3405312

dptZip=insee_first_name_dpt.zip
fileName=dpt2015.txt
url="https://www.insee.fr/fr/statistiques/fichier/2540004/dpt2015_txt.zip"

mkdir -p $dir
cd $dir

curl $url > $dptZip
unzip -o $dptZip
rm $dptZip

if [ $(wc -l $fileName | awk '{print $1}') -eq $count ]; then
        echo file correctly downloaded
else
        echo download failed..
fi
