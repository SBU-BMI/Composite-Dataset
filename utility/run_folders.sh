#!/bin/bash

file_path="/data/shared/tcga_analysis/seer_data/results"
for i in `cat current_caseid_list`; do
        for j in `ls $file_path/$i/`; do
        k=`grep wsi $file_path/$i/$j/*-algmeta.csv | head -n 1 | awk -F ',' '{print $24}'`;
                echo $i,$j,$k;
        done
done
