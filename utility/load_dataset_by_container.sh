#!/bin/bash
echo "-----------------------------------------------------"
echo "Date: $(date)                     Host:$(hostname)"
echo "-----------------------------------------------------"


PROGNAME=$(basename "$0")

echo "enter image list file: "
read input_file
IFS=$'\n' read -d '' -r -a caseid_list < $input_file
echo $caseid_list;

#container storage folder:/data/seer_project/img

dataPath="/data/seer_project/img/composite_results"
container_datapath="/data/images/composite_results" 

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
     echo $case_id_path;
     new_path="${case_id_path/"/data/seer_project/img/composite_results"/$container_datapath}"
     echo "$new_path";
     echo "------------------------------------------------------------------------------"
     docker exec -it quip-loader /usr/local/pathomics_featuredb/src/build/install/featuredb-loader/bin/featuredb-loader \
       --inptype csv --fromdb --dbname quip_comp --dbhost  quip-data --quip $new_path/      
     #echo "========="           
      
    #echo "-------------------"      
   fi
done;



echo "-----------------------------------------------------"
echo "Date: $(date)"
echo "-----------------------------------------------------"
