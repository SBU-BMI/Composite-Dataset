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
import datetime


    
    
if __name__ == '__main__':
  if len(sys.argv)<0:
    print "usage:python composite_dataset2.py ";
    exit();    
  
  start_time = time.time();
  #my_home=os.getcwd(); 
  my_home="/home/bwang/shapely"    
  storage_node="nfs004";
  
  
  start_time = time.time();    
  csv.field_size_limit(sys.maxsize);  
  
  
  main_dir =os.path.join(my_home, 'results/'); 
  out_dir  =os.path.join(my_home, 'composite_results/');  
  db_host="quip3.bmi.stonybrook.edu";
  db_port="27017";   
  max_workers=16;#multiple process number
  
  client = MongoClient('mongodb://'+db_host+':'+db_port+'/'); 
   
  db = client.quip;
  objects = db.objects;
  metadata=db.metadata; 
  images=db.images;
  
  db2=client.quip_comp; 
  objects2 = db2.objects;
  metadata2=db2.metadata; 
  images2=db2.images;
    
  print "============== find new images collection ====================" 
  #f = open('myfile', 'w')
  #f.write('hi there\n')  # python will convert \n to os.linesep
  #f.close()  # you can omit in most cases as the destructor will call it
  #file_name="case_id_list_"+datetime.datetime.today().strftime('%Y_%m_%d');
  #print file_name;
  #exit();  
  #f = open(file_name, 'w')
  #header='case_id,execution_id,annotation_count\n';  
  #f.write(header);
  image_user_list=[];     
  total_image_count=0; 
  excluded_list=['joseph.balsamo','tahsin.kurc','tigerfan7495','joelhsaltz','tammy.diprima','siwen.statistics']
  for myimage in images.find({},{"_id":0,"case_id":1}):
    case_id=myimage["case_id"];
    subject_id=case_id;
    user_list =[];
    tmp_image_user=[[],[]];
    for mymeta in metadata.find({"image.case_id":case_id,
                                 "provenance.analysis_execution_id":{'$regex':'_composite_input'}}):
      case_id2=mymeta["image"]["case_id"];
      execution_id = mymeta["provenance"]["analysis_execution_id"]; 
      tmp_user =execution_id.split('_')[0];      
      user_list.append(tmp_user);       
    for exclued_user in excluded_list:
      if exclued_user in user_list:
        user_list.remove(exclued_user);
    user_count=len(user_list); 
    if(user_count>0):
     for user in user_list:
       new_execution_id=user +"_composite_input";
       annot_count=objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": new_execution_id
                                      }).count();   
       if(annot_count>0): 
         tmp_image_user[0]= case_id;
         tmp_image_user[1]= new_execution_id;
         image_user_list.append(tmp_image_user);
         if(user_count==1):     
           print  case_id,new_execution_id,annot_count; 
           line_str=str(case_id)+','+new_execution_id + ','+str(annot_count)+'\n';                      
         if(user_count > 1):     
           print  case_id,new_execution_id,annot_count,"more than one user"; 
           line_str=str(case_id)+','+new_execution_id + ','+ str(annot_count)+',more than one user\n';
         #f.write(line_str);        
         total_image_count+=1;         
    print "-------------------------------------";    
  print total_image_count;
  #print image_user_list;
  #f.close()
  #exit();
  
  total_image_count2=0;
  total_image_count=0;
  total_metadata_count=0;
  object_count=0;
  object_count2=0;  
  
  print "=== find missed images list =============="
  image_user_list2=[];
  for image_user in image_user_list:
    case_id = image_user[0];
    subject_id=case_id;
    execution_id = image_user[1];  
    #print case_id, execution_id;
    
    tmp_user =execution_id.split('_')[0];
    execution_id_new =  tmp_user +"_composite_dataset" 
    
    image_count =images2.find({"case_id":case_id}).count();
    if (image_count>0):
      total_image_count+=1;
    if (image_count ==0):
      print case_id, execution_id;
      print "           NOT find this image in image collection" 
      
        
    metadata_count= metadata2.find({"image.case_id":case_id,
                                    "provenance.analysis_execution_id":execution_id}).count();
    if (metadata_count>0):
      total_metadata_count+=1; 
    if (metadata_count ==0):
      print case_id, execution_id;
      print "         NOT find this image in metadata collection"  
                                     
    annot_count=objects2.find({"provenance.image.case_id":case_id,
                               "provenance.image.subject_id":subject_id,
                               "provenance.analysis.execution_id": execution_id
                              }).count();                                
    if (annot_count>0):
      object_count+=1; 
    if (annot_count ==0):
      print case_id, execution_id;
      print "        NOT find this image in object collection with id composite_input"  
      
    annot_count2 =0  
    for composite_dataset in objects2.find({"provenance.image.case_id":case_id,
                                            "provenance.image.subject_id":subject_id,
                                            "provenance.analysis.execution_id": execution_id_new
                              }).limit(1):                                
      object_count2+=1;
      annot_count2 =1;
      
    if (composite_dataset is None):
      print case_id, execution_id;
      print "        NOT find this image in objects collection with is composite_dataset"  
              
    if(image_count ==0 or metadata_count ==0 or   annot_count ==0 or   annot_count2==0): 
      tmp_image_user=[[],[]];
      tmp_image_user[0]= case_id;
      tmp_image_user[1]= execution_id;              
      image_user_list2.append(tmp_image_user);  
      total_image_count2+=1;  
      
    #if(annot_count >0  and image_count==0):
      #print str(case_id) + ' ' + str(execution_id) +  ":  no images data but with objects data"; 
      
    #if(annot_count >0 and metadata_count==0):
      #print str(case_id) + ' ' + str(execution_id) +  ":  no metadata data but with objects data";  
      
                         
  #print image_user_list, 
  print "-----";
  print total_image_count
  print total_metadata_count
  print object_count 
  print object_count2
  print total_image_count2
  
  print "------------------"
  print len(image_user_list2);
  print "============================="
  print image_user_list2;
  
  
  file_name="case_id_list_"+datetime.datetime.today().strftime('%Y_%m_%d'); 
  f = open(file_name, 'w')
  header='case_id,user\n';  
  f.write(header);
  for image_user in image_user_list2:
    case_id = image_user[0];
    execution_id = image_user[1]; 
    user =execution_id.split('_')[0];
    total_area=0.0
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                    "provenance.image.subject_id":case_id,
                                    "provenance.analysis.execution_id": execution_id
                                      }):
      polygon=annotation["geometry"]["coordinates"][0];
      tmp_poly=[tuple(i) for i in polygon];
      annotation_polygon1 = Polygon(tmp_poly);
      annotation_polygon_1 = annotation_polygon1.buffer(0);
      total_area = total_area + annotation_polygon_1.area;
    total_area2 = "{0:.8f}".format(total_area);
    #print case_id, total_area2,user;
    print case_id, user;     
    #line_str=str(case_id)+','+user + ','+ total_area2 + '\n';
    line_str=str(case_id)+','+user + '\n';
    f.write(line_str); 
  f.close()  
  
  """
  total_image_count2=0;
  for image_user in image_user_list:
    case_id = image_user[0];
    subject_id=case_id;
    execution_id = image_user[1];    
    for myimage2 in images2.find({"case_id":case_id}):
        #case_id3=myimage2["case_id"];
        #subject_id3=case_id3;
        print "find image "+ str(case_id) + " in quip_comp images collection";
        user_list =[];
        for mymeta2 in metadata2.find({"image.case_id":case_id,
                                       "provenance.analysis_execution_id":execution_id}):
          #case_id4=mymeta2["image"]["case_id"];
          #subject_id4=case_id4;
          execution_id_new = mymeta2["provenance"]["analysis_execution_id"]; 
          print "find image "+ str(case_id) + " in quip_comp metadata collection";
          if (execution_id != execution_id_new):
            print "exectution_in in quip is not the same as in quip_comp. " + str(execution_id) + ":"+str(execution_id_new);
          tmp_user =execution_id_new.split('_')[0];      
          user_list.append(tmp_user);                 
        for exclued_user in excluded_list:
          if exclued_user in user_list:
            user_list.remove(exclued_user);
        user_count=len(user_list); 
        if(user_count>0):
          for user in user_list:
            new_execution_id=user +"_composite_input";
            annot_count=objects2.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id
                                      }).count();   
            if(annot_count>0): 
              tmp_image_user=[[],[]];
              tmp_image_user[0]= case_id;
              tmp_image_user[1]= new_execution_id;              
              image_user_list.remove(tmp_image_user);  
              total_image_count2+=1;
              print "find image "+ str(case_id) + " in quip_comp objects collection";
              if(user_count==1): 
                print  case_id,new_execution_id,annot_count;                     
              if(user_count > 1):     
                print  case_id,new_execution_id,annot_count,"more than one user";             
  print image_user_list,
  #print len(image_user_list);
  print total_image_count2
  """  
  
  """
  print "========= search quip_comp database ==================================================";
  case_id_list_file = open(file_name, "r");  
  csvReader = csv.reader(case_id_list_file);
  header = csvReader.next();
  case_id_Index = header.index("case_id")
  execution_id_Index = header.index("execution_id") ;  
  total_image_count2=0;
  for row in csvReader:
      case_id = row[case_id_Index]
      execution_id = row[execution_id_Index]
      #print row_line,case_id, execution_id;      
      # search quip_comp database
      for myimage2 in images2.find({"case_id":case_id}):
        case_id3=myimage2["case_id"];
        subject_id3=case_id3;
        print "find image "+ str(case_id3) + " in quip_comp images collection";
        user_list =[];
        for mymeta2 in metadata2.find({"image.case_id":case_id3,
                                       "provenance.analysis_execution_id":{'$regex':'_composite_input'}}):
          case_id4=mymeta2["image"]["case_id"];
          subject_id4=case_id4;
          execution_id4 = mymeta2["provenance"]["analysis_execution_id"]; 
          print "find image "+ str(case_id4) + " in quip_comp metadata collection";
          tmp_user =execution_id4.split('_')[0];      
          user_list.append(tmp_user);                 
        for exclued_user in excluded_list:
          if exclued_user in user_list:
            user_list.remove(exclued_user);
        user_count=len(user_list); 
        if(user_count>0):
          for user in user_list:
            new_execution_id2=user +"_composite_input";
            annot_count=objects2.find({"provenance.image.case_id":case_id3,
                                      "provenance.image.subject_id":subject_id3,
                                      "provenance.analysis.execution_id": new_execution_id2
                                      }).count();   
            if(annot_count>0): 
              print "find image "+ str(case_id3) + " in quip_comp objects collection";
              if(user_count==1): 
                print  case_id3,new_execution_id2,annot_count;                     
              if(user_count > 1):     
                print  case_id3,new_execution_id2,annot_count,"more than one user";               
              total_image_count2+=1;
  print total_image_count2; 
  """
   
  """  
  # This script reads a GPS track in CSV format and
  #  prints a list of coordinate pairs
  import csv
 
  # Set up input and output variables for the script
  gpsTrack = open("C:\\data\\Geog485\\gps_track.txt", "r")
 
  # Set up CSV reader and process the header
  csvReader = csv.reader(gpsTrack)
  header = csvReader.next()
  latIndex = header.index("lat")
  lonIndex = header.index("long")
 
  # Make an empty list
  coordList = []
 
  # Loop through the lines in the file and get each coordinate
  for row in csvReader:
      lat = row[latIndex]
      lon = row[lonIndex]
      coordList.append([lat,lon])
 
  # Print the coordinate list
  print coordList
  """

  
  """
  #bwang@nfs001:/data/shared/tcga_analysis/seer_data/results 
  input_file="caseid_perfix_algorithm.txt"
  analysis_list_csv = os.path.join(my_home, input_file);         
  if not os.path.isfile(analysis_list_csv):
    remote_file = os.path.join(remote_folder, 'analysis_list.csv');
    subprocess.call(['scp', remote_file,analysis_list_csv]); 
      
  
  print '--- read master CSV file ---- ';  
  index=0;
  analysis_list=[];   
  case_id_list=[];
  with open(analysis_list_csv, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:      
      tmp_analysis_list=[[],[],[]]; 
      tmp_analysis_list[0]= each_row[0];
      tmp_analysis_list[1]= each_row[1];
      tmp_analysis_list[2]= each_row[2];   
      analysis_list.append(tmp_analysis_list); 
      case_id_list.append(each_row[0]);          
  print "total rows from master csv file is %d " % len(analysis_list) ; 
  case_id_list_new = sorted(set(case_id_list)); 
  print "uinque case_id count is %d " % len(case_id_list_new) ;
  print case_id_list_new;     
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
  for case_id in case_id_list_new:  
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
    #if (user_count >1 or user_count ==0):
      #print case_id,user_count, user_list     
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
      #print "no user for this case id!!!";
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
  
  """
  print "============== new code ====================" 
  for item in  process_list:
    case_id=item[0];
    user=item[1];
    new_execution_id=user +"_composite_dataset";
    metadata2.delete_many({"image.case_id":case_id,"provenance.analysis_execution_id":new_execution_id})
    print "----------------"
    print case_id,user;    
    for user_composite_input in  metadata2.find({"image.case_id":case_id,
                     "provenance.analysis_execution_id":{'$regex':'_composite_input'}},
                     {"provenance.analysis_execution_id":1}):
      execution_id=user_composite_input["provenance"]["analysis_execution_id"];
      tmp_user =execution_id.split('_')[0]; 
      if (tmp_user != user):
        print case_id,execution_id;       
        metadata2.delete_many({"image.case_id":case_id,
                              "provenance.analysis_execution_id":execution_id})
  """                              
  
  """
  print "============== get human markup annotations as output json file ===================="                              
  with open('result3.json', 'w') as fp: 
    for item in  process_list:
      case_id=item[0];
      subject_id =case_id; 
      user=item[1];
      execution_id=user +"_composite_input";
      print case_id,subject_id,execution_id;
      for annotation in objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id
                                      },{"_id":0}):            
        polygon=annotation["geometry"]["coordinates"][0]; 
        x=polygon[0][0];
        y=polygon[0][1];
        if(x<0.0 or x>1.0):
          continue;
        if(y<0.0 or y>1.0):
          continue;  
        json.dump(annotation, fp); 
        #objects2.insert_one(annotation);     
  """
  
  """
  print "============== get human markup annotations ===================="
  record_count=0;
  for item in  process_list[1:]:
    case_id=item[0];
    subject_id =case_id; 
    user=item[1];
    execution_id=user +"_composite_input";
    print case_id,subject_id,execution_id;
    for annotation in objects.find({"provenance.image.case_id":case_id,
                                      "provenance.image.subject_id":subject_id,
                                      "provenance.analysis.execution_id": execution_id
                                      },{"_id":0}):            
      polygon=annotation["geometry"]["coordinates"][0]; 
      x=polygon[0][0];
      y=polygon[0][1];
      if(x<0.0 or x>1.0):
        continue;
      if(y<0.0 or y>1.0):
        continue;        
      objects2.insert_one(annotation);
      record_count+=1;
  print "total annotations inserted is %d" % record_count;
  """
  """
  print "============== clear images collection ====================" 
  total_image_count=0;
  deleted_image_count=0;
  for record in images2.find({},{"_id":0,"case_id":1}):
    case_id=record["case_id"];
    total_image_count+=1;
    find_case_id=0; 
    for item in  process_list:
      case_id_2=item[0];    
      if (case_id == case_id_2):
        find_case_id=1; 
        break;          
    if (find_case_id==0):             
      images2.delete_many({"case_id":case_id});
      print case_id ;
      deleted_image_count+=1;
  print  total_image_count, deleted_image_count; 
  """
