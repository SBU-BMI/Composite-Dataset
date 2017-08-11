print('----starting -----')
#from shapely.geometry import Point
#patch = Point(0.0, 0.0).buffer(10.0)
#print patch

from pymongo import MongoClient
import pprint


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


polygon_algorithm =[[0 for y in xrange(2)] for x in xrange(3000)]

index=0;
for annotation in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": execution_id_array[0]
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):
  algorithm=annotation["algorithm"]          
  #pprint.pprint(annotation["algorithm"])
  #print 'algorithm is:'+algorithm;
  polygon=annotation["geometry"]["coordinates"][0]
  array_size=len(polygon);
  #print "array size:"+str(array_size);
  polygon_algorithm[index][0]=algorithm;
  polygon_algorithm[index][1]=polygon;  
  #print index, polygon_algorithm[index][0],len(polygon_algorithm[index][1]);
  index+=1;
  


test_algorithm = polygon_algorithm[20][0];
test_poly=polygon_algorithm[20][1];

print test_poly;


#for poly_computer in collection.find({"provenance.image.case_id":case_id,
 #             "provenance.image.subject_id":subject_id,
  #           "provenance.analysis.execution_id":test_algorithm,
   #          x : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
    #         y : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}  
     #       }):pprint.pprint(poly_computer);
  
for poly_computer in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id":test_algorithm ,
             "x" : { '$gte':test_poly[0][0], '$lte':test_poly[2][0]},
             "y" : { '$gte':test_poly[0][1], '$lte':test_poly[2][1]}
            },{"_id":0,"geometry.coordinates":1,"algorithm":1}):pprint.pprint(poly_computer);
    
  
  
#for annotationtmp in  polygon_algorithm:
#  print annotationtmp[0],len(annotationtmp[1]);
  
   

#print polygon[0] ;
#index=0;
#for point in  polygon:
#  print index,point;
#  index+=1;  
 
