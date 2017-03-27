#!/bin/bash

dir=../dojo-spark/src/main/resources/data/inscriptions

count=274691

fileName=inscriptions_etudiants.csv

url="https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-atlas_regional-effectifs-d-etudiants-inscrits/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true"

mkdir -p $dir

cd $dir

curl -o $fileName $url

if [ $(wc -l $fileName | awk '{print $1}') -eq $count ]; then
	echo file correctly downloaded
else
	echo download failed..
fi
