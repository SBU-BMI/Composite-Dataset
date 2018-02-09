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
    print "usage:python syn_db_175_to_quip3.py  image_list_file";
    exit();    
  
  image_list_file=sys.argv[1];
   
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
  
  print " --- read input image list --- ";  
  image_list = os.path.join(image_list_file);
  image_id_list=[];  
  with open(image_list, 'r') as input_file:
    reader=input_file.readlines(); 
    image_id_reader = list(reader);     
  for image_id_str in image_id_reader:
    image_id=image_id_str.replace('\n','');    
    image_id_list.append(image_id); 
  print "image_id_list is %s ." % image_id_list;
  print "image_id_list length is %d ." % len(image_id_list);
      
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  process_list =[];    
  for case_id in image_id_list:  
    subject_id=case_id; 
    print case_id; 
    user_list =[];
    tmp_process_list_item=[[],[]];    
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
  #exit(); 
  
    
  """
  print "============== syn  images collection ===================="  
  for item in  process_list:
    case_id=item[0];    
    dest_images_count= images2.find({"case_id":case_id}).count();
    if (dest_images_count ==0):
      for image_record in images.find({"case_id":case_id},{"_id":0}):
        images2.insert_one(image_record);
        print str(case_id)+ "has been added now.";
    else:
      print str(case_id) +" is Not empty in quip3 server db quip_comp images collection";    
  exit();    
  """
  
  
  """
  print "============== syn  metadata collection ===================="  
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_input";
    execution_id2=user +"_composite_dataset";
    #remove old records 
    metadata2.remove({"image.case_id":case_id});
    for metadata_record in metadata.find({"image.case_id":case_id,"provenance.analysis_execution_id":execution_id},{"_id":0}):
      metadata2.insert_one(metadata_record); 
      print str(case_id)+ " has been added now for execution_id.";
    for metadata_record in metadata.find({"image.case_id":case_id,"provenance.analysis_execution_id":execution_id2},{"_id":0}):
      metadata2.insert_one(metadata_record);
      print str(case_id)+ " has been added now for execution_id2.";  
  exit(); 
  """     
  
  
  print "============== syn  objects collection ===================="  
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_input";
    execution_id2=user +"_composite_dataset";     
    composite_input_count=objects2.find({"provenance.image.case_id":case_id,
                                           "provenance.image.subject_id":subject_id,
                                           "provenance.analysis.execution_id": {'$regex' : 'composite_input', '$options' : 'i'}}).count();  
    composite_dataset_count=objects2.find({"provenance.image.case_id":case_id,
                                           "provenance.image.subject_id":subject_id,
                                           "provenance.analysis.execution_id": {'$regex' : 'composite_dataset', '$options' : 'i'}}).count();
    print case_id,composite_input_count,composite_dataset_count
   
    
  """
  print "============== syn human markup annotations ===================="   
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_input";               
    #remove old records in objects collection     
    objects2.remove({"provenance.image.case_id":case_id,
                     "provenance.image.subject_id":subject_id,
                     "provenance.analysis.execution_id": {'$regex' : 'composite_input', '$options' : 'i'}});  
    objects2.remove({"provenance.image.case_id":case_id,
                     "provenance.image.subject_id":subject_id,
                     "provenance.analysis.execution_id": {'$regex' : 'composite_dataset', '$options' : 'i'}});
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                    "provenance.image.subject_id":subject_id,
                                    "provenance.analysis.execution_id": execution_id
                                   },{"_id":0}):                                                
      objects2.insert_one(annotation);
    print str(case_id)+ " has been processed."; 
  """
  
  """
  print "============== clear existing records ===================="  
  for item in  process_list:
    case_id=item[0];    
    dest_images_count= images2.find({"case_id":case_id}).count();
    if (dest_images_count ==0):
      for image_record in images.find({"case_id":case_id},{"_id":0}):
        images2.insert_one(image_record);
        print case_id;
    else:
      print str(case_id) +" is Not empty in quip3 server db quip_comp images collection";    
  exit();
  """
  
  
  
  
  
                   
  exit();
  
  
  