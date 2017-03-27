#!/bin/bash

dir=../dojo-spark/src/main/resources/data/insee

dptZip=insee_first_name_dpt.zip

mkdir -p $dir
cd $dir

curl https://www.insee.fr/fr/statistiques/fichier/2540004/dpt2015_txt.zip > $dptZip
unzip -o $dptZip

rm $dptZip
