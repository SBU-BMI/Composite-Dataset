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

    
    
if __name__ == '__main__':
  if len(sys.argv)<2:
    print "usage:python composite_all.py result_location";
    exit();
    
  #case_id= sys.argv[1];#image_id or case_id 
  #user=sys.argv[2];
  my_home=sys.argv[1];#computing result location
  
  start_time = time.time();
  #print('-- main ---');  
  csv.field_size_limit(sys.maxsize);
  
  #my_home='/home/bwang/shapely/';
  main_dir =os.path.join(my_home, 'results/'); 
  out_dir  =os.path.join(my_home, 'composite_results/');
  db_host="quip3.bmi.stonybrook.edu";
  db_port="27017"; 
  max_workers=16;#multiple process number
  
  # copy master csv file analysis_list.csv from nfs001 node
  remote_folder="nfs001:/data/shared/tcga_analysis/seer_data/";  
  analysis_list_csv = os.path.join(my_home, 'analysis_list.csv');         
  if not os.path.isfile(analysis_list_csv):
    remote_file = os.path.join(remote_folder, 'analysis_list.csv');
    subprocess.call(['scp', remote_file,analysis_list_csv]);      
  
  client = MongoClient('mongodb://'+db_host+':'+db_port+'/');  
  db = client.quip;
  objects = db.objects;
  metadata=db.metadata;  
  
  print '--- read master CSV file ---- ';
  #prefixs_algorithm=[[0 for y in xrange(2)] for x in xrange(20)];
  index=0;
  analysis_list=[];  
  case_id_list=[];
  with open(analysis_list_csv, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:
      #case_id_row=each_row[0];
      #prefix=each_row[1];
      #algorithm_row=each_row[2]; 
      tmp_analysis_list=[[],[],[]]; 
      tmp_analysis_list[0]= each_row[0];
      tmp_analysis_list[1]= each_row[1];
      tmp_analysis_list[2]= each_row[2];   
      analysis_list.append(tmp_analysis_list);
      case_id_list.append(each_row[0]);      
  print "total rows from master csv file is %d " % len(analysis_list) ;  
  print analysis_list;
  
  case_id_list_new = sorted(set(case_id_list)); 
  print "uinque case_id count is %d " % len(case_id_list_new) ;
  print case_id_list_new;
  #exit();
  
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  process_list =[];  
  for case_id in case_id_list_new:
    #print " ------ case_id:  "+case_id + "---------------------------";
    user_list =[];
    tmp_process_list_item=[[],[]];
    for user_composite_input in  metadata.find({"image.case_id":case_id,
                     "provenance.analysis_execution_id":{'$regex':'_composite_input'}},
                     {"provenance.analysis_execution_id":1}):
      tmp=user_composite_input["provenance"]["analysis_execution_id"];
      tmp_user =tmp.split('_')[0];
      #print "  --- user:"+tmp_user;
      user_list.append(tmp_user);    
    for exclued_user in excluded_list:
      if exclued_user in user_list:
        user_list.remove(exclued_user);
    user_count=len(user_list);
    if user_count >1:  
      #print " ------ case_id:  "+case_id + "---------------------------";  
      #print "user list of this case_id are: "+str(user_list);
      #print "user number of this case id is %d: " % len(user_list);
      user = user_list[0];# get first user
      #print "first user of this case_id is "+user;      
      if ( case_id == "17033063"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='lillian.pao';
        process_list.append(tmp_process_list_item);  
      elif ( case_id == "BC_056_0_1"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='areeha.batool';
        process_list.append(tmp_process_list_item); 
      elif ( case_id == "BC_061_0_2"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='lillian.pao';
        process_list.append(tmp_process_list_item); 
      elif ( case_id == "BC_065_0_2"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='epshita.das';
        process_list.append(tmp_process_list_item); 
      elif ( case_id == "PC_227_2_1"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='areeha.batool';
        process_list.append(tmp_process_list_item);        
    elif user_count<1: 
      print " ------ case_id:  "+case_id + "---------------------------";
      print "no user for this case id!!!"; 
    else:#only one user
      #print "only one user of this case_id is "+str(user_list);  
      #print ""; 
      tmp_process_list_item[0]=case_id;
      tmp_process_list_item[1]=user_list[0];
      process_list.append(tmp_process_list_item);
  print "final user and case_id combination is  %d " % len(process_list) ; 
  print process_list; 
  #exit();   
  
  def getPrefix(case_id,algorithm):
    prefix="";
    for tmp in analysis_list:
      case_id_row=tmp[0];
      prefix_row=tmp[1];
      algorithm_row=tmp[2];
      if (case_id_row == case_id and algorithm_row == algorithm):
          prefix =prefix_row;                   
          tmp_prefixs_algorithm[1]=prefix;
          tmp_prefixs_algorithm[0]=algorithm;
          prefixs_algorithm.append(tmp_prefixs_algorithm);
          #print "prefix is "+prefix;
          break
    return  prefix;      
          
  print " --- ----  start the loop of  case_id  and user combination ---";  
  for tmp_user_case_id in  process_list:
    case_id=tmp_user_case_id[0];
    user=tmp_user_case_id[1];
    prefixs_algorithm=[];
    tmp_prefixs_algorithm =[[],[]]; 
    print " --- case_id  and user are %s / %s  -------" % (case_id,user);
    #case_id="17035671";
    #case_id="17033057";
    subject_id=case_id;
    #user="helen.wong";
    #user="lillian.pao";
    execution_id=user +"_composite_input"; 
    new_execution_id=user +"_composite_dataset";
    print " --- execution_id  and new_execution_id are %s and %s  -------" % (execution_id,new_execution_id);
    #exit();   
    algorithms=[];
    prefixs=[]; 
    total_involve_tile=0;  
    for algorithm in objects.distinct("algorithm",{"provenance.image.case_id":case_id,
                                             "provenance.image.subject_id":subject_id,
                                             "provenance.analysis.execution_id":execution_id}): 
      algorithms.append(algorithm); 
      #print algorithms; 
    if len(algorithms) <1:
      print "No objects data associated with this image.";
      break;
    #copy all csv and json file from nfs001 node
    print ' --- copy all csv and json file from nfs001 node ---- ';
    start_copy_time = time.time(); 
    prefix="";        
    for algorithm in algorithms:      
      print "--- algorithm is " +algorithm + "------"
      prefix=getPrefix(case_id,algorithm);
      #print prefix ;
      if(prefix==""):
        print "not find prefix" ; 
        #break;
        exit(); 
      remote_result = os.path.join(remote_folder, 'results/');
      remote_img_folder = os.path.join(remote_result, case_id);
      detail_remote_folder = os.path.join(remote_img_folder, prefix);    
      local_img_folder = os.path.join(main_dir, case_id);
      local_folder = os.path.join(local_img_folder, prefix);
      if not os.path.exists(local_folder):
        print '%s folder do not exist, then create it.' % local_folder;
        os.makedirs(local_folder);
        
      if os.path.isdir(local_folder) and len(os.listdir(local_folder)) > 0: 
        print " all csv and json files have been copied from data node.";
      else:
        subprocess.call(['scp', detail_remote_folder+'/*.json',local_folder]);
        subprocess.call(['scp', detail_remote_folder+'/*features.csv',local_folder]);       
        
    total_copy_time = time.time() - start_copy_time;  
    print "total time to copy all csv and json file from nfs001 node is "+str(total_copy_time/60.00)+ ' minutes.';print "total time:"+str(total_copy_time/60.00)+ ' minutes.';
  
  exit();   
