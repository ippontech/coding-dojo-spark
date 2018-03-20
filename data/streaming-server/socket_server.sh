#!/bin/bash

while true
do
    genre=$(( ( RANDOM % 2 )  + 1 ))
    annee=$(( ( RANDOM % 50 )  + 1950 ))
    number=$(( ( RANDOM % 150 )  + 1 ))
    dpt=$(( ( RANDOM % 95 )  + 1 ))
    nameIndex=$(( ( RANDOM % 100) + (genre - 1) * 100 + 1))
    name=`sed "${nameIndex}q;d" /mnt/c/Dev/names.txt`
    echo "$genre $name $annee $dpt $number"
    sleep 1
done