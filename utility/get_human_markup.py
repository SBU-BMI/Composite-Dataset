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
  if len(sys.argv)<0:
    print "usage:python get_human_markup.py";
    exit();    
  
  start_time = time.time();  
  my_home="/home/bwang/shapely"    
  storage_node="nfs003";    
  
  main_dir =os.path.join(my_home, 'results/'); 
  out_dir  =os.path.join(my_home, 'composite_results/');  
  db_host="quip3.bmi.stonybrook.edu";
  db_port="27017";   
  max_workers=16;#multiple process number
  
  #copy master csv file analysis_list.csv from nfs001 node
  remote_folder="nfs001:/data/shared/tcga_analysis/seer_data/";  
  remote_result = os.path.join(remote_folder, 'results/');
  remote_composite_result = os.path.join(remote_folder, 'composite_results/');   
  
  analysis_list_csv = os.path.join(my_home, 'analysis_list.csv');         
  if not os.path.isfile(analysis_list_csv):
    remote_file = os.path.join(remote_folder, 'analysis_list.csv');
    subprocess.call(['scp', remote_file,analysis_list_csv]);      
  
  client = MongoClient('mongodb://'+db_host+':'+db_port+'/');  
  db = client.quip;
  objects = db.objects;
  metadata=db.metadata;   
  
  print '--- read master CSV file ---- ';  
  index=0;
  analysis_list=[];   
  caseid_list=[];
  with open(analysis_list_csv, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:      
      tmp_analysis_list=[[],[],[]]; 
      tmp_analysis_list[0]= each_row[0];
      tmp_analysis_list[1]= each_row[1];
      tmp_analysis_list[2]= each_row[2];   
      analysis_list.append(tmp_analysis_list); 
      caseid_list.append(each_row[0]);          
  print "total rows from master csv file is %d " % len(analysis_list) ;      
  caseid_list_new = sorted(set(caseid_list))
  donelist=['17035671','17033057','17033075','17032547','17032548']
  for case_id in donelist:
    caseid_list_new.remove(case_id); 
  
  print caseid_list_new;
  print len(caseid_list_new);
  #exit();  
  
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
    
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']  
  process_list =[]; 
  with open('result2.json', 'w') as fp:   
    for case_id in caseid_list_new:  
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
        if ( case_id == "17033063"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';
          #process_list.append(tmp_process_list_item);  
        elif ( case_id == "BC_056_0_1"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='areeha.batool';
          #process_list.append(tmp_process_list_item); 
        elif ( case_id == "BC_061_0_2"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='lillian.pao';
          #process_list.append(tmp_process_list_item); 
        elif ( case_id == "BC_065_0_2"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='epshita.das';
          #process_list.append(tmp_process_list_item); 
        elif ( case_id == "PC_227_2_1"):
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]='areeha.batool';
          #process_list.append(tmp_process_list_item); 
        else:# assign first user
          tmp_process_list_item[0]=case_id;
          tmp_process_list_item[1]=user;
          #process_list.append(tmp_process_list_item);           
      elif user_count<1: 
        #print " ------ case_id:  "+case_id + "---------------------------";
        print "no user for this case id!!!";
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]=""; 
      else:#only one user           
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]=user_list[0];
    
      if (user_count > 0):#user is available
        execution_id=tmp_process_list_item[1] +"_composite_input";                                         
        tmp_algorithm_list=[];
        annotation_count=0; 
        print '----- get human markups for this image and this user -----';   
         
        if ( case_id != "BC_388_1_2"):                                     
          for annotation in objects.find({"provenance.image.case_id":case_id,
                                          "provenance.image.subject_id":subject_id,
                                          "provenance.analysis.execution_id": execution_id
            },{"_id":0}):
            
            polygon=annotation["geometry"]["coordinates"][0]; 
            algorithm=annotation["algorithm"];  
            tmp_algorithm_list.append(algorithm);                                         
            x=polygon[0][0];
            y=polygon[0][1];
            if(x<0.0 or x>1.0):
              continue;
            if(y<0.0 or y>1.0):
              continue;  
            polygon_algorithm[annotation_count][0]=algorithm;
            polygon_algorithm[annotation_count][1]=polygon;       
            annotation_count+=1;  
            json.dump(annotation, fp) ;   
        
