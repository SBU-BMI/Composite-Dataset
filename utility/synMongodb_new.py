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
    print "usage:python synMongodb_new.py  caseid";
    exit();    
  
  case_id=sys.argv[1];  
   
  db_host="129.49.249.175";
  db_host2="quip3.bmi.stonybrook.edu";
  
  db_port="27017";   
  
  client  = MongoClient('mongodb://'+db_host+':'+db_port+'/');  
  client2 = MongoClient('mongodb://'+db_host2+':'+db_port+'/');  
  
  db = client.quip;  
  objects = db.objects;
  metadata = db.metadata; 
  images = db.images;
  
  db2=client2.quip_comp; 
  objects2 = db2.objects;  
  metadata2 = db2.metadata;
  images2 = db2.images; 
  
  case_id_list_new=[case_id];    
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  process_list =[];    
  for case_id in case_id_list_new:  
    subject_id=case_id;  
    user_list =[];
    tmp_process_list_item=[[],[]];    
    prefixs_algorithm=[];
    polygon_algorithm=[[0 for y in xrange(2)] for x in xrange(1000)]; 
    for user_composite_input in  metadata2.find({"image.case_id":case_id,
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
      elif ( case_id == "BC_056_0_1"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='areeha.batool';        
      elif ( case_id == "BC_061_0_2"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='lillian.pao';        
      elif ( case_id == "BC_065_0_2"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='epshita.das';        
      elif ( case_id == "PC_227_2_1"):
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]='areeha.batool';         
      else:# assign first user
        tmp_process_list_item[0]=case_id;
        tmp_process_list_item[1]=user;                  
    elif user_count<1:      
      tmp_process_list_item[0]=case_id;
      tmp_process_list_item[1]=""; 
    else:#only one user           
      tmp_process_list_item[0]=case_id;
      tmp_process_list_item[1]=user_list[0];
    
    if (user_count > 0):#user is available                                                      
      process_list.append(tmp_process_list_item);               
  print "final user and case_id combination is  %d " % len(process_list) ; 
  #print process_list 
  print "------------ case_id list  ----------------------------"
  for item in  process_list:
    case_id=item[0];
    user=item[1];
    print case_id, user;   
    
  print "============== syn composite dataset ====================" 
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_dataset";
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                    "provenance.image.subject_id":subject_id,
                                    "provenance.analysis.execution_id": execution_id
                                   },{"_id":0}):                                                
      objects2.insert_one(annotation);     
    
