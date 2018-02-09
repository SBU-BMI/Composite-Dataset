#!/bin/bash

input_file="case_id_list_2018_01_24"

IFS=$'\n' read -d '' -r -a caseid_list < $input_file

for case_id in "${caseid_list[@]}"
do  
  qsub -v caseid=$case_id synMongodb_new.pbs 
  #echo $case_id
  #break 
done
