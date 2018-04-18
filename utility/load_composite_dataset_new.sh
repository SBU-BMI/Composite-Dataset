#!/bin/bash
echo "-----------------------------------------------------"
echo "Date: $(date)                     Host:$(hostname)"
echo "-----------------------------------------------------"


PROGNAME=$(basename "$0")

input_file="case_id_list_2018_4_18"
IFS=$'\n' read -d '' -r -a caseid_list < $input_file
echo $caseid_list;
#quip3 host mount
#"Source": "/data/seer_project/img",
#"Destination": "/data/images",

dataPath="/data/seer_project/img/composite_results"
container_datapath="/data/images/composite_results" 
record_count=0


find_caseid() {
    caseid_path="$1"
    find_caseid=0    
    
    for case_id in "${caseid_list[@]}"
    do
       #echo $case_id;
         if test "${caseid_path#*$case_id}" != "$caseid_path"
           then             
              find_caseid=1
              break        
        fi     
    done
    
    if [[ $find_caseid == 1 ]]
      then  echo 1 
    else echo -1
    fi   
}


for case_id_path in $(find  $dataPath -maxdepth 1 -mindepth 1 -type d); 
do 
  
  #echo $case_id_path;
  
  if [[ $(find_caseid "$case_id_path" ) == 1 ]] 
   then
     for prefix in $(find  $case_id_path/* -type d );
      do
        echo "$prefix"; 
        new_path="${prefix/"/data/seer_project/img/composite_results"/$container_datapath}"
        echo "$new_path";
        echo "------------------------------------------------------------------------------"
        docker exec -it quip-loader /usr/local/pathomics_featuredb/src/build/install/featuredb-loader/bin/featuredb-loader \
          --inptype csv --fromdb --dbname quip_comp --dbhost  quip-data --quip $new_path/
         record_count=$(($record_count+1))
        #echo "========="             
      done;
    #echo "-------------------"      
   fi
done;

echo $record_count

echo "-----------------------------------------------------"
echo "Date: $(date)"
echo "-----------------------------------------------------"
