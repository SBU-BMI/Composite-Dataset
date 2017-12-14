#!/bin/bash

myfunc() {
    string="$1"
    substring="$2"
    if test "${string#*$substring}" != "$string"
    then
        #echo "substring is in string"
        echo 1    # $substring is in $string
    else
        #echo "substring is NOT in string"
        echo -1    # $substring is not in $string
    fi
}


input_file="caseidlist"
IFS=$'\n' read -d '' -r -a caseid_list < $input_file
source_file_path="/data/seer_project/img"
#record_count=0
for case_id in "${caseid_list[@]}"
do 
  #echo $case_id.svs
  svs_file_path=`find $source_file_path -name $case_id.svs`;  
  echo $svs_file_path
  if [[ $(myfunc "$svs_file_path" "BC_056_0_1") == 1 ]]; 
   then  
     scp $svs_file_path feiqiao@129.49.249.175:/home/feiqiao/quip2/img/.  
     #record_count=$(($record_count+1))
  fi
done
