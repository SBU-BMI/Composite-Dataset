from pymongo import MongoClient
from shapely.geometry import LineString
from shapely.geometry.polygon import LinearRing
from shapely.geometry import Polygon
from bson import json_util 
import numpy as np
import time
import pprint
import json 
import csv
import sys
import os
import shutil
import concurrent.futures
import subprocess
import pipes

    
    
if __name__ == '__main__':
  if len(sys.argv)<1:
    print "usage:python find_caseid_prefix.py caseid";
    exit();    
  
  start_time = time.time();  
  my_home="/data1/bwang"  
  #my_home="/home/bwang/shapely"   
  case_id = sys.argv[1];      
 
  image_id_list=[case_id];    
  print "image_id_list is %s ." % image_id_list;
  
  start_time = time.time();    
  csv.field_size_limit(sys.maxsize);  
  
  main_dir =os.path.join(my_home, 'results/'); 
  out_dir  =os.path.join(my_home, 'composite_results2/');  
  db_host="quip3.bmi.stonybrook.edu";
  db_port="27017";   
    
  # copy master csv file analysis_list.csv from nfs001 node
  remote_folder="nfs001:/data/shared/tcga_analysis/seer_data/";  
  remote_result = os.path.join(remote_folder, 'results/');
  remote_composite_result = os.path.join(remote_folder, 'composite_results/');   
  
  input_file="caseid_perfix_algorithm.txt"
  analysis_list_csv = os.path.join(my_home, input_file);         
  if not os.path.isfile(analysis_list_csv):
    print "caseid_perfix_algorithm.txt is NOT available." 
    exit();     
  
  client = MongoClient('mongodb://'+db_host+':'+db_port+'/');  
  db = client.quip;
  objects = db.objects;
  metadata=db.metadata;   
  
  #print '--- read master CSV file ---- ';  
  index=0;
  analysis_list=[];   
  with open(analysis_list_csv, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:      
      tmp_analysis_list=[[],[],[]]; 
      tmp_analysis_list[0]= each_row[0];
      tmp_analysis_list[1]= each_row[1];
      tmp_analysis_list[2]= each_row[2];   
      analysis_list.append(tmp_analysis_list);           
  #print "total rows from master csv file is %d " % len(analysis_list) ;      
  
  def getPrefix(case_id,algorithm):
    prefix="";    
    for tmp in analysis_list:
      case_id_row=tmp[0];
      prefix_row=tmp[1];
      algorithm_row=tmp[2];
      if (case_id_row == case_id and algorithm_row == algorithm):
          prefix =prefix_row;     
          break
    return  prefix; 
  
  caseid_prefix_file = os.path.join(my_home, 'caseid_prefix');  
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  process_list =[]; 
  with open(caseid_prefix_file, 'w') as f1:    
    for case_id in image_id_list:  
      subject_id=case_id;  
      user_list =[];
      tmp_process_list_item=[[],[],[],[],[]];    
      prefixs_algorithm=[];
      polygon_algorithm=[[0 for y in xrange(2)] for x in xrange(1000)]; 
      for user_composite_input in  metadata.find({"image.case_id":case_id,
                     "provenance.analysis_execution_id":{'$regex':'_composite_input'}},
                     {"provenance.analysis_execution_id":1}):
        tmp=user_composite_input["provenance"]["analysis_execution_id"];
        tmp_user =tmp.split('_')[0];      
        user_list.append(tmp_user);    
      for exclued_user in excluded_list:
        if exclued_user in user_list:
          user_list.remove(exclued_user);
      user_count=len(user_list);    
      if user_count >1:  
        user = user_list[0];# get first user               
        if ( case_id == "17033056"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='nikol.palomino';          
        elif ( case_id == "PC_136_1_1"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='bryan.jeanty';        
        elif ( case_id == "17032547"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';        
        elif ( case_id == "PC_054_0_1"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='bryan.jeanty';        
        elif ( case_id == "BC_065_0_1"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='bryan.jeanty';     
        elif ( case_id == "17039831"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';      
        elif ( case_id == "17039832"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';
        elif ( case_id == "17039870"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';  
        elif ( case_id == "17039886"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';
        elif ( case_id == "17039907"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';
        elif ( case_id == "17039864"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';                   
        else:# assign first user
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]=user;            
      elif user_count<1: 
        print " ------ case_id:  "+case_id + "---------------------------";
        print "no user for this case id!!!";
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]=""; 
      else:#only one user           
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]=user_list[0];
    
      if (user_count > 0):#user is available
        execution_id=tmp_process_list_item[1] +"_composite_input";  
        prefix_list="";
        for tmp_algorithm in objects.distinct("algorithm",{"provenance.image.case_id":case_id,
                                            "provenance.image.subject_id":subject_id,
                                            "provenance.analysis.execution_id":execution_id}): 
          prefix=getPrefix(case_id,tmp_algorithm);
          prefix_list+=prefix+ ' ';      
        row=case_id+','+prefix_list;
        f1.write(row+ os.linesep) ; 
        #print row ;      
  
  
  
  
   
