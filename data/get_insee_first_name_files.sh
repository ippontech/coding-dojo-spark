#!/bin/bash

dir=insee
natZip=insee_first_name_nat.zip
dptZip=insee_first_name_dpt.zip

mkdir -p $dir
cd $dir

curl https://www.insee.fr/fr/statistiques/fichier/2540004/nat2015_txt.zip > $natZip
curl https://www.insee.fr/fr/statistiques/fichier/2540004/dpt2015_txt.zip > $dptZip

unzip -o $natZip
unzip -o $dptZip

rm $natZip
rm $dptZip
