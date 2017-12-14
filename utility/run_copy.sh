#!/bin/bash


for i in `cat result_a.txt`; do
        a=`echo $i | awk -F '/' '{print $7}'`;
        echo $a;
        mkdir $a;
        rsync -av $i $a/;
done
