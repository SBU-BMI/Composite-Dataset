
  How to generate composite dataset from Camicroscope human markup
  
  the source code is in eagle.bmi.stonybrook.edu , folder  /home/bwang/shapely
  to get composite dataset,
  sh run_jobs.sh
  which invokes files run_jobs.pbs ,
                find_caseid_prefix_new.py , 
                caseid_perfix_algorithm , 
                run_jobs.py

 Before execute run_jobs.sh,  create image list file , which contains image case_id  and put it at the same folder 
 as run_jobs.sh.   
 for example, input file case_id_list_2017_11_09 in run_jobs file  is list of image id 
 and 
 create new folder composite_dataset at eagle nfs004 server in folder  /data/shared/bwang/
  
 the composite dataset is stored at  eagle.bmi.stonybrook.edu nfs004 server in folder  /data/shared/bwang/composite_results
 
 After composite dataset is created, following the steps described in readme2.txt to complete the rest procedures.
 
 
