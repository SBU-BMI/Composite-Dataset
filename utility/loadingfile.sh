#!/bin/bash

#copy segmen result files from storage node
DATAHOST="nfs001"
#LOCALHOME="/data1/bwang"
LOCALHOME="/home/bwang/shapely"
LOCALDIR_RESULTS=$LOCALHOME/results
LOCALDIR_COMPOSITE_RESULTS=$LOCALHOME/composite_results
DATADIR="/data/shared/tcga_analysis/seer_data/results"
master_csv_file_path="/data/shared/tcga_analysis/seer_data/analysis_list.csv"
input_file="caseidlist"
IFS=$'\n' read -d '' -r -a caseid_list < $input_file

master_csv_file="analysis_list.csv"
if [ ! -f $master_csv_file ]; then
    echo "Master CSV File not found!"
    scp $DATAHOST:$master_csv_file_path ./analysis_list.csv
fi

for case_id in "${caseid_list[@]}"
do
  
  prefix_str=`grep $case_id  $master_csv_file | awk -F ',' '{print $2}'`;  
  prefix_array=($prefix_str)  
  tLen=${#prefix_array[@]} 
  for (( i=0; i<${tLen}; i++ ));
  do
    prefix=${prefix_array[$i]}    
    local_folder=$LOCALDIR_RESULTS/$case_id/$prefix
    if [ ! -d "$local_folder" ]; then      
      mkdir -p $local_folder
    fi    
    remote_folder=$DATAHOST:$DATADIR/$case_id/$prefix
    json_remote_files=$remote_folder/*.json
    scp $json_remote_files $local_folder/
    csv_remote_files=$remote_folder/*features.csv
    scp $csv_remote_files $local_folder/
  done  
done
exit 0
