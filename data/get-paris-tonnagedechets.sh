#!/bin/bash -e

mkdir paris-tonnagesdechets && cd $_

curl http://opendata.paris.fr/explore/dataset/tonnages_des_dechets_bacs_jaunes/download/?format=csv > tonnages_des_dechets_bacs_jaunes.csv
