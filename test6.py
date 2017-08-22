
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
  
  xx=55296;
  yy=63488;
  
  for index1 in range (0,total_algorithms-1):
    prefix = prefixs_algorithm[index1][0];  
    algorithm = prefixs_algorithm[index1][1];
    img_folder = os.path.join(main_dir, case_id);
    #print img_folder;
    algorithm_folder = os.path.join(img_folder, prefix);     
    print algorithm_folder;
    #print os.listdir(algorithm_folder);
    if os.path.isdir(algorithm_folder) and len(os.listdir(algorithm_folder)) > 0:
      #json_filename2 = [f for f in os.listdir(algorithm_folder) if f.endswith('.json')][0] ;
      json_filename = [f for f in os.listdir(algorithm_folder) if f.endswith('.json') and \
                              f.split('_')[-2] == 'x' + str(xx) and f.split('_')[-1] == 'y' + str(yy) + '-algmeta.json'][0];
      
      #print json_filename2; 
      print json_filename;                       
      #json_filename_list = [f for f in os.listdir(algorithm_folder) if f.endswith('.json')] ;
      #print len(json_filename_list);
      
      csv_filename = [f2 for f2 in os.listdir(algorithm_folder) if f2.endswith('features.csv') and \
                              f2.split('_')[-2] == 'x' + str(xx) and f2.split('_')[-1] == 'y' + str(yy) + '-features.csv'][0];
      #csv_filename_list = [f2 for f2 in os.listdir(algorithm_folder) if f2.endswith('features.csv')] ;
      #print len(csv_filename_list);
      print csv_filename;
      
      with open(os.path.join(algorithm_folder, json_filename)) as f:
        data = json.load(f);
        analysis_id=data["analysis_id"];
        image_width=data["image_width"];
        image_height=data["image_height"];
        tile_minx=data["tile_minx"];
        tile_miny=data["tile_miny"];
        tile_width=data["tile_width"];
        tile_height=data["tile_height"];
        title_polygon=[[float(tile_minx)/float(image_width),float(tile_miny)/float(image_height)],[float(tile_minx+tile_width)/float(image_width),float(tile_miny)/float(image_height)],[float(tile_minx+tile_width)/float(image_width),float(tile_miny+tile_height)/float(image_height)],[float(tile_minx)/float(image_width),float(tile_miny+tile_height)/float(image_height)],[float(tile_minx)/float(image_width),float(tile_miny)/float(image_height)]];
        print title_polygon;
                
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
        new_polygon=[];      
        for polygon in column['Polygon']:         
          #print str(polygon);
          #print polygon.split(':');
          tmp_str=str(polygon);
          #print tmp_str;
          #print '\n';
          tmp_str=tmp_str.replace('[','');
          tmp_str=tmp_str.replace(']','');
          #print tmp_str;
          #print '\n';
          split_str=tmp_str.split(':');
          #print split_str;
          #print '\n';
          #print len(split_str);
          #for x,y in split_str:
          #print image_width;
          #print image_height;
          for i in range(0, len(split_str)-1, 2):
            point=[float(split_str[i])/image_width,float(split_str[i+1])/image_height];
            new_polygon.append(point);
            #new_polygon.append('[',x,y,']');
            #print new_polygon;
            #print x,y
          #print new_polygon;
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
        
        tmp_case_id=data["case_id"];
        tmp_subject_id=data["subject_id"]; 
        
        print image_width,image_height;
        print tile_minx,tile_miny;
        print tile_width,tile_height;
        print tmp_case_id,tmp_subject_id;
        
               
        if data["analysis_id"] == algorithm:
          mask_filename = [f for f in os.listdir(algorithm_folder) if f.endswith('.png') and \
                                 f.split('_')[-2] == 'x' + str(xx) and f.split('_')[-1] == 'y' + str(yy) + '-seg.png'][0];
          #print f.split('_') ; 
          #print f.split('_')[-2];
          #print f.split('_')[-1];                      
          break
          
  print mask_filename; 
  
  
  print '----- get human markups by one user -----'; 
  polygon_algorithm=[[0 for y in xrange(2)] for x in xrange(200)]; 
  index=0;  
  for annotation in objects.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": execution_id
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):  
    polygon=annotation["geometry"]["coordinates"][0];
    algorithm=annotation["algorithm"];
    polygon_algorithm[index][0]=algorithm;
    polygon_algorithm[index][1]=polygon;  
    index+=1;
  print 'total annotation number is %d' % index; 
  total_annotation_count = index;
  #print polygon_algorithm[0][1];
  print 'this polygon point count is %d' % len(polygon_algorithm[0][1]); 
  
  print '-- find all annotations NOT within another annotation  -- ';
  polygon_algorithm_final=[[0 for y in xrange(3)] for x in xrange(200)]; 
  index3=0
  for index1 in range(0, total_annotation_count-1):
    algorithm = polygon_algorithm[index1][0]; 
    annotation = polygon_algorithm[index1][1]; 
    tmp_poly=[tuple(i) for i in annotation];
    annotation_polygon1 = Polygon(tmp_poly);
    annotation_polygon_1 = annotation_polygon1.buffer(0);
    polygonBound= annotation_polygon_1.bounds;
    array_size=len(annotation);
    print '-----------------------------------------------------------------';
    print "annotation index %d and annotation point size %d" % (index1, array_size) ;
    is_within=False;
    for index2 in range(0, total_annotation_count-1):
      annotation2 = polygon_algorithm[index2][1]; 
      tmp_poly2=[tuple(j) for j in annotation2];
      annotation_polygon2 = Polygon(tmp_poly2);
      annotation_polygon_2 = annotation_polygon2.buffer(0);
      if not annotation_polygon_1.equals(annotation_polygon_2):    
        if (annotation_polygon_1.within(annotation_polygon_2)):    
          is_within=True;
          polygonBound2= annotation_polygon_2.bounds;
          array_size2=len(annotation2); 
          print polygonBound;     
          print '-- find within polygon --';        
          print "annotation index %d and annotation point size %d" % (index2, array_size2) ;
          print polygonBound2;
          print '--------';    
    if not is_within:
      print 'add annotation of index %d' % index1;
      polygon_algorithm_final[index3][0]=algorithm; 
      polygon_algorithm_final[index3][1]=annotation;
      polygon_algorithm_final[index3][2]=index1;
      index3+=1;
  print 'final annotation number is %d' % index3;  
  #print  polygon_algorithm_final[35][2];   
  final_total_annotation_count=index3;
  print final_total_annotation_count;
  
  print '------ get all tiles as polygon ------ ';
  title_polygon_algorithm_final=[[0 for y in xrange(3)] for x in xrange(10000)];
  print total_algorithms;
  for index1 in range (0,total_algorithms-1):
    prefix = prefixs_algorithm[index1][0];  
    algorithm = prefixs_algorithm[index1][1];
    img_folder = os.path.join(main_dir, case_id);
    #print img_folder;
    algorithm_folder = os.path.join(img_folder, prefix);     
    print algorithm_folder;
    #print os.listdir(algorithm_folder);
    if os.path.isdir(algorithm_folder) and len(os.listdir(algorithm_folder)) > 0:                            
      json_filename_list = [f for f in os.listdir(algorithm_folder) if f.endswith('.json')] ;
      print len(json_filename_list);
      tmp_title_polygon=[];
      for json_filename in json_filename_list:     
        with open(os.path.join(algorithm_folder, json_filename)) as f:
          data = json.load(f);
          analysis_id=data["analysis_id"];
          image_width=data["image_width"];
          image_height=data["image_height"];
          tile_minx=data["tile_minx"];
          tile_miny=data["tile_miny"];
          tile_width=data["tile_width"];
          tile_height=data["tile_height"];
          title_polygon=[[float(tile_minx)/float(image_width),float(tile_miny)/float(image_height)],[float(tile_minx+tile_width)/float(image_width),float(tile_miny)/float(image_height)],[float(tile_minx+tile_width)/float(image_width),float(tile_miny+tile_height)/float(image_height)],[float(tile_minx)/float(image_width),float(tile_miny+tile_height)/float(image_height)],[float(tile_minx)/float(image_width),float(tile_miny)/float(image_height)]];
          print title_polygon;
          tmp_title_polygon.append(title_polygon);
        
    title_polygon_algorithm_final[index1][0]=prefix;
    title_polygon_algorithm_final[index1][1]=algorithm;
    title_polygon_algorithm_final[index1][2]=tmp_title_polygon ;
    print '----------------------------------------------------';
    #print  title_polygon_algorithm_final[index1][2]; 
    print  len(title_polygon_algorithm_final[index1][2]);      
        
  
  print '--- end ---';
