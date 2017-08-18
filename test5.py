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
import os

# main_dir = '/data01/shared/tcga_analysis/seer_data/results
# s_id is the slide id of the image, name of the sub directory in main_dir
# xx and yy are the location of the 2048*2048 tile
# algo is the algorithm we are looking for

def load_tile_mask_SEER(main_dir, s_id, xx, yy, algo):  # read the tile segmentation mask

    img_folder = os.path.join(main_dir, s_id)

    for algo_folder in os.listdir(img_folder):
        if os.path.isdir(os.path.join(img_folder, algo_folder)) and \
                        len(os.listdir(os.path.join(img_folder, algo_folder))) > 0:
            filename = [f for f in os.listdir(os.path.join(img_folder, algo_folder)) if f.endswith('.json')][0]
            with open(os.path.join(img_folder, algo_folder, filename)) as f:
                data = json.load(f)
            if data["analysis_id"] == algo:
                mask_filename = [f for f in os.listdir(os.path.join(img_folder, algo_folder)) if f.endswith('.png') and
                                 f.split('_')[-2] == 'x' + str(xx) and f.split('_')[-1] == 'y' + str(yy) + '-seg.png'][0]
                break

    return imread(os.path.join(img_folder, algo_folder, mask_filename))
    
    
if __name__ == '__main__':
  print('-- main ---');
  #main_dir = '/data01/shared/tcga_analysis/seer_data/results';
  main_dir ='/home/bwang/shapely/results/17035671/';
  s_id="17035671";
  algo ="wsi:r1.1:w0.8:l3:u200:k20:j1"; 
  analysis_list_csv="/home/bwang/shapely/analysis_list.csv";
  prefixs_algorithm=[[0 for y in xrange(2)] for x in xrange(20)];
    
  # write list to CSV file, it works!
  #the_list=["car","house","wife"]
  #with open("my_file.csv", 'w') as outfile:
    #writer = csv.writer(outfile, delimiter='\t')
    #writer.writerow(the_list)
 
  start_time = time.time();

  db_host="quip3.bmi.stonybrook.edu"
  db_port="27017"

  client = MongoClient('mongodb://'+db_host+':'+db_port+'/')
  #client = MongoClient('mongodb://quip3.bmi.stonybrook.edu:27017/')
  db = client.quip;
  objects = db.objects;
  metadata=db.metadata;

  case_id="17035671"
  subject_id="17035671"
  execution_id="helen.wong_composite_input"    
  
  algorithms=[];
  prefixs=[];
  for algorithm in objects.distinct("algorithm",{"provenance.image.case_id":case_id,
                                             "provenance.image.subject_id":subject_id,
                                             "provenance.analysis.execution_id":execution_id}): 
    algorithms.append(algorithm);
    #print algorithm;
  #print algorithms;
  #print algorithms[0],algorithms[1];
  
  # read CSV file & load into list
  index=0;
  with open(analysis_list_csv, 'r') as my_file:
    reader = csv.reader(my_file, delimiter=',')
    my_list = list(reader);
    for each_row in my_list:
      case_id_row=each_row[0];
      prefix=each_row[1];
      algorithm_row=each_row[2];      
      if (case_id_row == case_id and algorithm_row in algorithms):
        #print case_id,prefix,algorithm_row;
        prefixs.append(prefix); 
        prefixs_algorithm[index][0] = prefix;
        prefixs_algorithm[index][1] = algorithm_row;
        index+=1;
  print prefixs_algorithm;
  total_algorithms=index;
  print total_algorithms
  #exit();
  #print prefixs;
  #print algorithms; 
  xx=30720;
  yy=79872;
  for index1 in range (0,total_algorithms-1):
    prefix = prefixs_algorithm[index1][0];  
    algorithm = prefixs_algorithm[index1][1];
    img_folder = os.path.join(main_dir, case_id);
    #print img_folder;
    algorithm_folder = os.path.join(img_folder, prefix);     
    print algorithm_folder;
    #print os.listdir(algorithm_folder);
    if os.path.isdir(algorithm_folder) and len(os.listdir(algorithm_folder)) > 0:
      json_filename = [f for f in os.listdir(algorithm_folder) if f.endswith('.json')][0] ;
      json_filename_list = [f for f in os.listdir(algorithm_folder) if f.endswith('.json')] ;
      #print len(json_filename_list);
      
      csv_filename = [f2 for f2 in os.listdir(algorithm_folder) if f2.endswith('features.csv')][0] ;
      csv_filename_list = [f2 for f2 in os.listdir(algorithm_folder) if f2.endswith('features.csv')] ;
      #print len(csv_filename_list);
      
      
      with open(os.path.join(algorithm_folder, csv_filename)) as f2:
        reader = csv.reader(f2);
        headers = reader.next();
        #print headers;
        column = {};
        for h in headers:
          column[h] = [];
        #print column;
        for row in reader:
          for h, v in zip(headers, row):
            column[h].append(v);
        #print column;        
        print len(column['Polygon']);        
        for polygon in column['Polygon']:         
          print str(polygon);
          print polygon.split(':');
          break;     


      
      with open(os.path.join(algorithm_folder, json_filename)) as f:
        data = json.load(f);
        analysis_id=data["analysis_id"];
        image_width=data["image_width"];
        image_height=data["image_height"];
        tile_minx=data["tile_minx"];
        tile_miny=data["tile_miny"];
        tile_width=data["tile_width"];
        tile_height=data["tile_height"];
        tile_width=data["tile_width"];
        tile_height=data["tile_height"];
        tmp_case_id=data["case_id"];
        tmp_subject_id=data["subject_id"];  
               
        if data["analysis_id"] == algorithm:
          mask_filename = [f for f in os.listdir(algorithm_folder) if f.endswith('.png') and \
                                 f.split('_')[-2] == 'x' + str(xx) and f.split('_')[-1] == 'y' + str(yy) + '-seg.png'][0];
          #print f.split('_') ; 
          #print f.split('_')[-2];
          #print f.split('_')[-1];                      
          break
  print mask_filename;   
  print '--- end ---';
