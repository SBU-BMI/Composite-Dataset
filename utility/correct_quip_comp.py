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
    print "usage:python corrent_quip_comp.py"
    exit();    
  
   
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
  
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics'];
  record_count=0;
  for image_record in images2.find({},{"_id":0}):
    case_id=image_record["case_id"];    
    composite_input_count=metadata2.find({"image.case_id":case_id,
                                          "image.subject_id":case_id,
				                                  "provenance.analysis_execution_id":{'$regex' : 'composite_input', '$options' : 'i'}}).count(); 				 
    composite_dataset_count=metadata2.find({"image.case_id":case_id,
                                           "image.subject_id":case_id,
				                                   "provenance.analysis_execution_id":{'$regex' : 'composite_dataset', '$options' : 'i'}}).count();
    user_list =[];
    user="";
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
        user='lillian.pao';       
      elif ( case_id == "BC_056_0_1"):
        user='areeha.batool';        
      elif ( case_id == "BC_061_0_2"):
        user='lillian.pao';        
      elif ( case_id == "BC_065_0_2"):
        user='epshita.das';        
      elif ( case_id == "PC_227_2_1"):
        user='areeha.batool';         
      else:# assign first user
        user=user;                  
    elif user_count<1:      
      user=""; 
    else:#only one user           
      user=user_list[0];    
    if (user_count > 0):#user is available                                                      
      #print case_id,user
      record_count+=1;
    
    execution_id=user +"_composite_input"; 
    execution_id2=user +"_composite_dataset"; 
    markup_count=0; 
    dataset_count=0;   
    for markup in objects2.find({"provenance.image.case_id":case_id,
                                 "provenance.image.subject_id":case_id,
                                 "provenance.analysis.execution_id":execution_id}).limit(1):
      markup_count+=1;                             
    for markup in objects2.find({"provenance.image.case_id":case_id,
                                 "provenance.image.subject_id":case_id,
                                 "provenance.analysis.execution_id":execution_id2}).limit(1):
      dataset_count+=1;  
      
    if (composite_input_count==0 or composite_dataset_count==0 or markup_count ==0 or dataset_count==0):                                                                                                                         
      print case_id,composite_input_count,composite_dataset_count,markup_count,dataset_count;
      """
      for metadata_record in metadata.find({"image.case_id":case_id,
                                            "image.subject_id":case_id,
				                                    "provenance.analysis_execution_id":{'$regex' : 'composite_dataset', '$options' : 'i'}},{"_id":0}):
        metadata2.insert_one(metadata_record);
      """  
  print record_count;
  exit();
  
  
  
    
  """  
  print "============== syn  images collection ===================="  
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
  
  """
  print "============== syn  metadata collection ===================="  
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id;       
    #remove old records and update to current record
    metadata2.remove({"image.case_id":case_id});    
    for metadata_record in metadata.find({"image.case_id":case_id},{"_id":0}):
      metadata2.insert_one(metadata_record);         
  exit();    
  """
  
  """
  print "============== syn human markup annotations ===================="   
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_input";       
    #remove old records in objects collection     
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                    "provenance.image.subject_id":subject_id,
                                    "provenance.analysis.execution_id": execution_id
                                   },{"_id":0}):                                                
      objects2.insert_one(annotation);            
  exit();
  """
