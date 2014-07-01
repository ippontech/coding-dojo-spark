#!/bin/bash -e

mkdir paris-arbresalignementparis2010 && cd $_

curl http://opendata.paris.fr/explore/dataset/arbresalignementparis2010/download/?format=csv > arbresalignementparis2010.csv
tail -n +2 arbresalignementparis2010.csv > arbresalignementparis2010_noheader.csv
