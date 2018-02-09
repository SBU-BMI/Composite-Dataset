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
    print "usage:python syn_db_quip_to_quip_comp.py  image_list_file";
    exit();    
  
  image_list_file=sys.argv[1];   
  
  db_host="quip3.bmi.stonybrook.edu";  
  db_port="27017"; 
      
  client  = MongoClient('mongodb://'+db_host+':'+db_port+'/');      
    
  db = client.quip;  
  objects = db.objects;
  metadata = db.metadata; 
  images = db.images;
  
  db2=client.quip_comp; 
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
  #exit();
  
  
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  process_list =[];    
  for case_id in image_id_list:  
    subject_id=case_id;  
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
    print  case_id, user_count;       
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
  #exit(); 
  
  """
  print "============== syn  images collection ===================="  
  for item in  process_list:
    case_id=item[0];       
    #syn metadata collection
    dest_images_count= images2.find({"case_id":case_id}).count();
    if (dest_images_count ==0):
      for image_record in images.find({"case_id":case_id},{"_id":0}):
        images2.insert_one(image_record);
        print str(case_id) +" is empty in quip_comp db images collection. So add info to db.";
    else:
      print str(case_id) +" is Not empty in quip_comp db images collection";    
  exit();    
  """
  
  
  print "============== syn  metadata collection ===================="  
  for item in  process_list:
    case_id=item[0];
    subject_id =case_id;       
    #syn metadata collection
    #remove old records and update to current record
    metadata2.remove({"image.case_id":case_id});    
    dest_metadata_count =metadata2.find({"image.case_id":case_id}).count();
    if (dest_metadata_count==0):  
      print str(case_id) +" is empty in quip_comp db metadata collection. So add info to db.";    
      for metadata_record in metadata.find({"image.case_id":case_id},{"_id":0}):
        metadata2.insert_one(metadata_record);        
    else:
      print str(case_id) +" is Not empty in quip_comp db metadata collection";    
  #exit();    
  
  
  
  print "============== syn human markup annotations ===================="   
  for item in  process_list:    
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_input";       
    #syn objects collection  
    #remove old records and update to current record       
    objects2.remove({"provenance.image.case_id":case_id,
                     "provenance.image.subject_id":subject_id,
                     "provenance.analysis.execution_id": execution_id}) ;
                     
    dest_objects_count=objects2.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id}).count();
    if (dest_objects_count==0):
      print str(case_id) +" is empty in quip_comp db objects collection. So add it to db.";           
      for annotation in objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id
                                   },{"_id":0}):                                                
        objects2.insert_one(annotation);                       
    else:
      print str(case_id) +" is Not empty in quip_comp db objects collection and annotation count is %d." %dest_objects_count;  
      print execution_id;     
  exit();
  
  
  """
  print "============== syn composite dataset ===================="   
  for item in  process_list:    
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];    
    execution_id=user +"_composite_dataset";       
    #syn objects collection   
    #remove old records and update to current record  
    objects2.remove({"provenance.image.case_id":case_id,
                     "provenance.image.subject_id":subject_id,
                     "provenance.analysis.execution_id": execution_id});
                                     
    dest_objects_count=objects2.find({"provenance.image.case_id":case_id,
                                     "provenance.image.subject_id":subject_id,
                                     "provenance.analysis.execution_id": execution_id}).count();
    #print case_id,dest_objects_count;                                    
    if (dest_objects_count==0):
      print str(case_id) +" is empty in 175 server db objects collection";
            
      for annotation in objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id
                                   },{"_id":0}):                                                
        objects2.insert_one(annotation); 
              
    else:
      print str(case_id) +" is Not empty in 175 server db objects collection";        
  exit();
  """
    
    
    
  
