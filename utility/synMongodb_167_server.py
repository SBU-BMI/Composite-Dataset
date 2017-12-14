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
    print "usage:python synMongodb_167_server.py  caseid";
    exit();    
  
  case_id=sys.argv[1];  
  my_home="/home/bwang/shapely"   
  
  db_host="quip3.bmi.stonybrook.edu";  
  db_host2="129.49.249.167";
  db_port="27017"; 
   
  client  = MongoClient('mongodb://'+db_host+':'+db_port+'/');  
  client2 = MongoClient('mongodb://'+db_host2+':'+db_port+'/');   
  
  db = client.quip;  
  objects = db.objects;
  metadata = db.metadata; 
  images = db.images;
  
  db2=client2.quip; 
  objects2 = db2.objects;  
  metadata2 = db2.metadata;
  images2 = db2.images; 
  
  input_file0="case_id_prefix_for_167_server"
  analysis_list_csv0 = os.path.join(my_home, input_file0);         
  if not os.path.isfile(analysis_list_csv0):
    print "case_id_prefix_for_167_server file is not available."
    exit();  
  
  print '--- read case_id_prefix file ---- ';  
  analysis_list0=[];   
  with open(analysis_list_csv0, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:      
      tmp_analysis_list=[[],[]]; 
      tmp_analysis_list[0]= each_row[0];
      tmp_analysis_list[1]= each_row[1];        
      analysis_list0.append(tmp_analysis_list);                
  print "total rows from master csv file is %d " % len(analysis_list0) ; 
    
  prefix_list=[];    
  for tmp in analysis_list0:
    case_id_row=tmp[0];
    prefix_row=tmp[1];      
    if (case_id_row == case_id):
        prefix_list.append(prefix_row);         
  print  case_id,prefix_list ;  
  
  input_file="caseid_perfix_algorithm.txt"
  analysis_list_csv = os.path.join(my_home, input_file);         
  if not os.path.isfile(analysis_list_csv):
    print "caseid_perfix_algorithm file is not available."
    exit();  
  
  print '--- read master CSV file ---- ';  
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
  print "total rows from master csv file is %d " % len(analysis_list) ; 
  
  algorithm_list=[];
  for prefix in prefix_list:  
   for tmp in analysis_list:
      case_id_row=tmp[0];
      prefix_row=tmp[1];
      algorithm_row=tmp[2];
      if (case_id_row == case_id and prefix_row == prefix):
          algorithm_list.append(algorithm_row);
          break;
  print case_id,algorithm_list;    
  #exit();
  
  print "============== syn  metadata collection ===================="  
  for algorithm in  algorithm_list:    
    subject_id =case_id;  
    dest_metadata_count= metadata2.find({"image.case_id":case_id,"provenance.analysis_execution_id":algorithm}).count();  
    if (dest_metadata_count == 0):   
      for metadata_record in metadata.find({"image.case_id":case_id,"provenance.analysis_execution_id":algorithm},{"_id":0}):
        metadata2.insert_one(metadata_record);         
  #exit(); 
  
  """
  print "============== syn composite dataset ====================" 
  for algorithm in  algorithm_list:
    subject_id=case_id;
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                    "provenance.image.subject_id":subject_id,
                                    "provenance.analysis.execution_id":algorithm
                                   },{"_id":0}):                                                
      objects2.insert_one(annotation);     
  """
  print "============== syn markup dataset ===================="   
  for algorithm in  algorithm_list:
    subject_id=case_id;
    skip_record=False;
    if(case_id == "17032581" and algorithm == "wsi:r1.1:w0.8:l3:u200:k20:j1"):
      skip_record=True; 
    if(case_id == "17033050" and algorithm == "wsi:r1.1:w0.8:l3:u200:k20:j1"):
      skip_record=True; 
    print  skip_record; 
    if not skip_record: 
      index=0;
      record_count=objects.find({"provenance.image.case_id":case_id,
                                 "provenance.image.subject_id":subject_id,
                                 "provenance.analysis.execution_id":algorithm
                               }).count();     
      for annotation in objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id":algorithm
                                   },{"_id":0}):                                                
        objects2.insert_one(annotation);
        index+=1;
        print "complete "+str(index)+"/"+ str(record_count);
