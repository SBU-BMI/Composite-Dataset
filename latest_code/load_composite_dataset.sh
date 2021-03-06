#!/bin/bash
echo "-----------------------------------------------------"
echo "Date: $(date)                     Host:$(hostname)"
echo "-----------------------------------------------------"


PROGNAME=$(basename "$0")

input_file="case_id_list"

IFS=$'\n' read -d '' -r -a caseid_list < $input_file
echo $caseid_list;


dataPath="/home/feiqiao/shapely/composite_results"
CMD="find  $dataPath -maxdepth 1 -type d" 
record_count=0


find_caseid() {
    caseid_path="$1"
    find_caseid=0    
    
    for case_id in "${caseid_list[@]}"
    do
       #echo $case_id;
         if test "${caseid_path#*$case_id}" != "$caseid_path"
           then
              #echo "substring is in string"
              #echo 1    # $substring is in $string
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
        echo "------------------------------------------------------------------------------"
        featuredb-loader --inptype csv --fromdb --dbname quip --dbhost  localhost --quip $prefix/
         record_count=$(($record_count+1))
        #echo "=========" 
        #break    
      done;
    #echo "-------------------" 
    #break 
   fi
done;

echo $record_count


#featuredb-loader --inptype csv --fromdb --dbname quip_comp --dbhost  localhost --quip /home/feiqiao/shapely/composite_results/17033075/f899be38-f88e-414e-a786-254c72070d93/






echo "-----------------------------------------------------"
echo "Date: $(date)"
echo "-----------------------------------------------------"
