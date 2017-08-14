print('----starting -----')
#from shapely.geometry import Point
#patch = Point(0.0, 0.0).buffer(10.0)
#print patch

from pymongo import MongoClient
import pprint

from shapely.geometry import LineString
from shapely.geometry.polygon import LinearRing
from shapely.geometry import Polygon




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


polygon_algorithm =[[0 for y in xrange(3)] for x in xrange(3000)]

print '-- find all annotations created by one user  -- ';

index=0;
for annotation in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": execution_id_array[0]
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  algorithm=annotation["algorithm"]          
  #pprint.pprint(annotation["algorithm"])
  #print 'algorithm is:'+algorithm;
  polygon=annotation["geometry"]["coordinates"][0]
  polygon2=annotation["geometry"]["coordinates"]  
  array_size=len(polygon);
  print "array size:"+str(array_size);
  polygon_algorithm[index][0]=algorithm;
  polygon_algorithm[index][1]=polygon; 
  polygon_algorithm[index][2]=polygon2;  
  #print index, polygon_algorithm[index][0],len(polygon_algorithm[index][1]);
  index+=1;  


#test_algorithm = polygon_algorithm[20][0];
#test_poly      = polygon_algorithm[20][1];
#test_poly2     = polygon_algorithm[20][2];


test_algorithm = polygon_algorithm[2][0];
test_poly      = polygon_algorithm[2][1];
#test_poly2     = polygon_algorithm[2][2];

#print test_poly;
line1 = LineString(test_poly);
#line2 = LineString(test_poly2);
lineBound= line1.bounds;
#shape2= line2.bounds;
print lineBound;

linearing = LinearRing(test_poly);
ringBound= linearing.bounds;
print ringBound;

polygon = Polygon(test_poly);
polygonBound= polygon.bounds;
print polygonBound;


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
for poly_computer in collection.find({"provenance.image.case_id":case_id,
             "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id":test_algorithm ,
#             "x" : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
#             "y" : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}             
#              "x" : { '$gte':lineBound[0], '$lte':lineBound[2]},
#              "y" : { '$gte':lineBound[1], '$lte':lineBound[3]}
              "x" : { '$gte':ringBound[0], '$lte':ringBound[2]},
              "y" : { '$gte':ringBound[1], '$lte':ringBound[3]}
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  polygon=poly_computer["geometry"]["coordinates"][0]
  array_size=len(polygon);
  #print "polygon size:"+str(array_size);
  polygon_count+=1;
  #pprint.pprint(poly_computer);

print 'returned polygon count: %d' % polygon_count;

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
 
