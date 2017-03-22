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


dir=enseignement
cd ..
mkdir -p $dir 
cd $dir 

curl --compressed -O "https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-atlas_regional-effectifs-d-etudiants-inscrits/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true"  > enseignement.csv
