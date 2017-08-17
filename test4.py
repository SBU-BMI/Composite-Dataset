
from pymongo import MongoClient
from shapely.geometry import LineString
from shapely.geometry.polygon import LinearRing
from shapely.geometry import Polygon
from bson import json_util 
import numpy as np
import json 
import pprint
import time


start_time = time.time();

db_host="quip3.bmi.stonybrook.edu"
db_port="27017"

client = MongoClient('mongodb://'+db_host+':'+db_port+'/')
#client = MongoClient('mongodb://quip3.bmi.stonybrook.edu:27017/')
db = client.quip
collection = db.objects
metadata=db.metadata

case_id="17035671"
subject_id="17035671"
execution_id="helen.wong_composite_input"
execution_id_array=[]

#pprint.pprint(collection.find_one({"provenance.image.case_id":"17035671",
#              "provenance.image.subject_id":"17035671",
 #            "provenance.analysis.execution_id":"helen.wong_composite_input"
  #          },{"_id":0,"geometry.coordinates":1}));




#for annotation in collection.find({"provenance.image.case_id":case_id,
 #             "provenance.image.subject_id":"17035671",
  #           "provenance.analysis.execution_id":execution_id
   #         },{"_id":0,"geometry.coordinates":1}):pprint.pprint(annotation)



#for executionid in metadata.find({"image.case_id":"17035671","provenance.type":"human"},{"provenance.analysis_execution_id":1}):pprint.pprint(executionid)

metadata_cursor= metadata.find({"image.case_id":case_id,"provenance.type":"human"},{"provenance.analysis_execution_id":1})

for executionid in metadata_cursor:
  #pprint.pprint(executionid["provenance"]["analysis_execution_id"]);
  execution_id=executionid["provenance"]["analysis_execution_id"]
  #print execution_id
  execution_id_array.append(execution_id)
  
#print  'execution_id[0]:'+execution_id_array[0]
#print '\n'
#print  'execution_id[1]:'+execution_id_array[1]

print '-- find all annotations created by one user  -- ';
polygon_algorithm=[[0 for y in xrange(2)] for x in xrange(100000)];
polygon_algorithm_final=[[0 for y in xrange(3)] for x in xrange(100000)];
total_annotation_array=[];
final_annotation_array=[];

index=0;
for annotation in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": execution_id_array[0]
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):  
  polygon=annotation["geometry"]["coordinates"][0];
  algorithm=annotation["algorithm"];
  total_annotation_array.append(polygon);  
  polygon_algorithm[index][0]=algorithm;
  polygon_algorithm[index][1]=polygon; 
  index+=1;
print 'total annotation number is %d' % len(total_annotation_array);
total_annotation_count=len(total_annotation_array);

print '-- find all annotations NOT within another annotation  -- ';
print  polygon_algorithm_final[2]; 
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


fout=open("text.json", "w");
for index4 in range(0, final_total_annotation_count-1):
  algorithm = polygon_algorithm_final[index4][0]; 
  annotation = polygon_algorithm_final[index4][1]; 
  ann_index = polygon_algorithm_final[index4][2]; 
  tmp_poly=[tuple(i) for i in annotation];
  annotation_polygon = Polygon(tmp_poly);
  polygonBound= annotation_polygon.bounds;
  array_size=len(annotation);
  #print polygonBound;
  #if annotation_polygon.is_valid:
   # print 'annotaion is valid polygon';
  #else:
  #  print 'annotaion is NOT valid polygon';      
  polygon_count=0;
  selected_polygon_count=0;
  elminated_polygon_count=0;
  
  for poly_computer in collection.find({"provenance.image.case_id":case_id,
             "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id":algorithm ,
              "x" : { '$gte':polygonBound[0], '$lte':polygonBound[2]},
              "y" : { '$gte':polygonBound[1], '$lte':polygonBound[3]}
            }):
    polygon_small=poly_computer["geometry"]["coordinates"][0];
    if array_size>5:# freeline polygon
      #polygon_obj = LineString(polygon);
      tmp_poly_small=[tuple(i) for i in polygon_small];
      #tmp_poly=[tuple(np.array(i)) for i in polygon];
      polygon_obj = Polygon(tmp_poly_small);
      #polygon_obj = Polygon([tuple(np.array(i)) for i in polygon]);
      #array_size=len(polygon);
      #print "polygon size:"+str(array_size); 
      #pprint.pprint(poly_computer);  
      if (polygon_obj.intersects(annotation_polygon)):  
        #print 'this polygon within annotation: the index  is: %d' % polygon_count; 
        selected_polygon_count+=1;  
        json.dump(json_util.dumps(poly_computer), fout) 
      else:
        #print 'this polygon is outside of annotation!'; 
        elminated_polygon_count+=1;     
      #print polygon_obj.relate(annotation_polygon);   
    else:#rectangle 
      selected_polygon_count+=1;
      json.dump(json_util.dumps(poly_computer), fout)    
    polygon_count+=1;  
  print '-------------------------------------------';   
  print 'index of polygon is %d and seleted  polygon  count: %d' % (ann_index,selected_polygon_count);
  print 'total  polygon count: %d' % polygon_count;  
  print 'elinimated polygon count: %d' % elminated_polygon_count;

print '-----------------------------------------------------------------';
elapsed_time = time.time() - start_time  
print "total time:"+str(elapsed_time);

exit();
      
    
    

print '-- find all annotations NOT within another annotation  -- ';
index=0;
for annotation in total_annotation_array:
  tmp_poly=[tuple(i) for i in annotation];
  annotation_polygon1 = Polygon(tmp_poly);
  annotation_polygon_1 = annotation_polygon1.buffer(0);
  polygonBound= annotation_polygon_1.bounds;
  array_size=len(annotation);
  print '-----------------------------------------------------------------';
  print "annotation index %d and annotation point size %d" % (index, array_size) ;
  #print polygonBound;
  #print annotation_polygon_1.is_valid;
  is_within=False;
  index2=0;
  for annotation2 in total_annotation_array:
    tmp_poly2=[tuple(j) for j in annotation2];
    annotation_polygon2 = Polygon(tmp_poly2);
    annotation_polygon_2 = annotation_polygon2.buffer(0);
    #print annotation_polygon_2.is_valid;
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
    index2+=1;
  if not is_within:
    final_annotation_array.append(annotation);    
  index+=1;
  
print 'final annotation number is %d' % len(final_annotation_array);  
      
      
exit();
  
  

index=0;
for annotation in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": execution_id_array[0]
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  algorithm=annotation["algorithm"]          
  #pprint.pprint(annotation["algorithm"])
  #print 'algorithm is:'+algorithm;
  polygon=annotation["geometry"]["coordinates"][0]
  #polygon2=annotation["geometry"]["coordinates"]  
  array_size=len(polygon);
  print '-----------------------------------------------------------------';
  print "annotation index %d and annotation point size %d" % (index, array_size) ;
  #annotation_polygon = Polygon(polygon);
  #annotation_polygon = Polygon(polygon2);
  #annotation_polygon = LinearRing(polygon);  
  #if annotation_polygon.is_ring:
   # print 'annotaion is closed ring';
  #else:
  #  print 'annotation is not closed';          
  #polygon_algorithm[index][0]=algorithm;
  #polygon_algorithm[index][1]=polygon; 
  #polygon_algorithm[index][2]=polygon2;  
  #print index, polygon_algorithm[index][0],len(polygon_algorithm[index][1]);
  index+=1; 
  tmp_poly=[tuple(i) for i in polygon];
  annotation_polygon = Polygon(tmp_poly);
  polygonBound= annotation_polygon.bounds;
  print polygonBound;
  #if annotation_polygon.is_valid:
   # print 'annotaion is valid polygon';
  #else:
  #  print 'annotaion is NOT valid polygon';
      
  polygon_count=0;
  selected_polygon_count=0;
  elminated_polygon_count=0;
  
  if index>56 and array_size>5:
    for poly_computer in collection.find({"provenance.image.case_id":case_id,
             "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id":algorithm ,
#             "x" : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
#             "y" : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}             
#              "x" : { '$gte':lineBound[0], '$lte':lineBound[2]},
#              "y" : { '$gte':lineBound[1], '$lte':lineBound[3]}
#              "x" : { '$gte':ringBound[0], '$lte':ringBound[2]},
#              "y" : { '$gte':ringBound[1], '$lte':ringBound[3]}
              "x" : { '$gte':polygonBound[0], '$lte':polygonBound[2]},
              "y" : { '$gte':polygonBound[1], '$lte':polygonBound[3]}
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
      polygon_small=poly_computer["geometry"]["coordinates"][0];
      #polygon_obj = LineString(polygon);
      tmp_poly_small=[tuple(i) for i in polygon_small];
      #tmp_poly=[tuple(np.array(i)) for i in polygon];
      polygon_obj = Polygon(tmp_poly_small);
      #polygon_obj = Polygon([tuple(np.array(i)) for i in polygon]);
      #array_size=len(polygon);
      #print "polygon size:"+str(array_size); 
      #pprint.pprint(poly_computer);  
      if (polygon_obj.intersects(annotation_polygon)):  
        #print 'this polygon within annotation: the index  is: %d' % polygon_count; 
        selected_polygon_count+=1;   
      else:
        #print 'this polygon is outside of annotation!'; 
        elminated_polygon_count+=1;     
      #print polygon_obj.relate(annotation_polygon);    
      polygon_count+=1;     
    print 'total returned polygon count: %d' % polygon_count;
    print 'seleted  polygon count: %d' % selected_polygon_count;
    print 'elinimated polygon count: %d' % elminated_polygon_count;

print '-----------------------------------------------------------------';
elapsed_time = time.time() - start_time  
print "total time:"+str(elapsed_time);

exit();

#test_algorithm = polygon_algorithm[20][0];
#test_poly      = polygon_algorithm[20][1];
#test_poly2     = polygon_algorithm[20][2];


test_algorithm = polygon_algorithm[2][0];
test_poly      = polygon_algorithm[2][1];
#test_poly2     = polygon_algorithm[2][2];

#print test_poly;
tmp_poly=[tuple(i) for i in test_poly];
line = LineString(tmp_poly);
#line2 = LineString(test_poly2);
lineBound= line.bounds;
#shape2= line2.bounds;
print lineBound;
if line.is_valid:
  print 'annotaion is valid line';
else:
  print 'annotaion is NOT valid line';

tmp_poly=[tuple(i) for i in test_poly];
linearing = LinearRing(tmp_poly);
ringBound= linearing.bounds;
print ringBound;
if linearing.is_valid:
  print 'annotaion is valid ring';
else:
  print 'annotaion is NOT valid ring';

tmp_poly=[tuple(i) for i in test_poly];
#tmp_poly=[tuple(np.array(i)) for i in test_poly];
annotation_polygon = Polygon(tmp_poly);
#annotation_polygon = Polygon([tuple(np.array(i)) for i in test_poly]);
polygonBound= annotation_polygon.bounds;
print polygonBound;
if annotation_polygon.is_valid:
  print 'annotaion is valid polygon';
else:
  print 'annotaion is NOT valid polygon';
  
#exit();

#print test_poly,test_poly2;


#for poly_computer in collection.find({"provenance.image.case_id":case_id,
 #             "provenance.image.subject_id":subject_id,
  #           "provenance.analysis.execution_id":test_algorithm,
   #          x : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
    #         y : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}  
     #       }):pprint.pprint(poly_computer);
 
 
 
print '-- rectangle query -- ';     
polygon_count=0;
selected_polygon_count=0;
elminated_polygon_count=0;

for poly_computer in collection.find({"provenance.image.case_id":case_id,
             "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id":test_algorithm ,
#             "x" : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
#             "y" : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}             
#              "x" : { '$gte':lineBound[0], '$lte':lineBound[2]},
#              "y" : { '$gte':lineBound[1], '$lte':lineBound[3]}
#              "x" : { '$gte':ringBound[0], '$lte':ringBound[2]},
#              "y" : { '$gte':ringBound[1], '$lte':ringBound[3]}
              "x" : { '$gte':polygonBound[0], '$lte':polygonBound[2]},
              "y" : { '$gte':polygonBound[1], '$lte':polygonBound[3]}
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  polygon=poly_computer["geometry"]["coordinates"][0];
  #polygon_obj = LineString(polygon);
  tmp_poly=[tuple(i) for i in polygon];
  #tmp_poly=[tuple(np.array(i)) for i in polygon];
  polygon_obj = Polygon(tmp_poly);
  #polygon_obj = Polygon([tuple(np.array(i)) for i in polygon]);
  array_size=len(polygon);
  #print "polygon size:"+str(array_size); 
  #pprint.pprint(poly_computer);  
  if (polygon_obj.intersects(annotation_polygon)):  
    #print 'this polygon within annotation: the index  is: %d' % polygon_count; 
    selected_polygon_count+=1;   
  else:
    #print 'this polygon is outside of annotation!'; 
    elminated_polygon_count+=1;     
  #print polygon_obj.relate(annotation_polygon);    
  polygon_count+=1;   
print 'total returned polygon count: %d' % polygon_count;
print 'seleted  polygon count: %d' % selected_polygon_count;
print 'elinimated polygon count: %d' % elminated_polygon_count;

exit();

print '-- geospatial query -- ';  
polygon_count=0;  
for poly_computer2 in collection.find({"provenance.image.case_id":case_id,
                                       "provenance.image.subject_id":subject_id,
                                       "provenance.analysis.execution_id":test_algorithm,                                                      
                                       "geometry": {
                                             '$geoWithin': {
                                                           '$geometry': {
                                                                         'type' : "Polygon" ,
                                                                         'coordinates': test_poly2
                                                                       }
                                                         }
                                         }                                       
                   } ,{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  polygon=poly_computer2["geometry"]["coordinates"][0]
  array_size=len(polygon);
  print "polygon size:"+str(array_size);
  polygon_count+=1;
  #pprint.pprint(poly_computer2); 
 
print 'returned polygon count: %d' % polygon_count;
 
#for annotationtmp in  polygon_algorithm:
#  print annotationtmp[0],len(annotationtmp[1]);
  
   

#print polygon[0] ;
#index=0;
#for point in  polygon:
#  print index,point;
#  index+=1;  

exit();
 
