//how to generate composite dataset

step 0: python quip_comp_continue.py to get image list
    or manually create image case_id list file

step 1a 
 login eagle.bmi.stonybrook.edu
 cd /home/bwang/shapely
 create image list file , which contains image case_id at the same folder.   
 for example, input file case_id_list_2017_11_09 is list of image id
 
step 1b 
  create new folder composite_dataset at eagle nfs004 server in folder  /data/shared/bwang/
  
step 1c
  sh  run_jobs.sh which invokes files run_jobs.pbs ,find_caseid_prefix_new.py and run_jobs.py
 
step2: copy composite_results to 175 server in eagle nfs004 server, in folder  /data/shared/bwang/
 rsync -avr composite_results/ feiqiao@129.49.249.175:/home/feiqiao/shapely/composite_results/ 
 
 
step3: create zip file  in quip3 server or 175 server:
 175 server: /home/feiqiao/shapely/create_zip_files.sh 
 //remove duplicate folders from composite_results_zip  before  create zip file
 quip3 server: /data/home/bwang/create_zip_files.sh
 //remove duplicate folders from composite_results_zip  before  create zip file

step4: syn images,metadata and objectives  collections info
    at eagle.bmi.stonybrook.edu ~home/shapely/syn_db_quip3_to_175.py

step5:load composite dataset to mongo db
 /home/feiqiao/shapely/load_composite_dataset.sh
 
step 6:  syn images,metadata and objectives  collections info
    at eagle.bmi.stonybrook.edu ~home/shapely/syn_db_175_to_quip3.py 

step7: copy composite dataset from 175 mongo db to quip3 mongo db 
 in eagle server:
 /home/bwang/shapely/synMongodb_new.sh 
 /home/bwang/shapely/synMongodb_new.pbs 
 /home/bwang/shapely/synMongodb_new.py
 
step 8: copy composite_results to quip3 server
    in eagle nfs004 server, in folder  /data/shared/bwang/  
  rsync -avr composite_results/ bwang@Quip3.bmi.stonybrook.edu:/data/home/bwang/composite_results/ 
 //remove duplicate folders from composite_results  before  rsync
 
step 9: update table2.php at quip-viewer container at folder /var/www/html/table/table2.php
    to add new image to the list
	
	
