#!/bin/bash

input_file_old="old_image_list"
input_file_new="new_image_list"

IFS=$'\n' read -d '' -r -a caseid_list_new < $input_file_new

for case_id in "${caseid_list_new[@]}"
do  
  return_str=`grep $case_id  $input_file_old`;  
  if [ -n "$return_str" ]; then  
      echo "$case_id : duplicate case_id"     
  else 
     echo   $case_id  
  fi  
    
done
