

print('----starting -----')
#from shapely.geometry import Point
#patch = Point(0.0, 0.0).buffer(10.0)
#print patch

from pymongo import MongoClient
import pprint


db_host="bmi.stonybrook.edu"
db_port=27017

client = MongoClient('mongodb://'+db_host+':'+db_port+'/')
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

metadata_cursor= metadata.find({"image.case_id":"17035671","provenance.type":"human"},{"provenance.analysis_execution_id":1})
for executionid in metadata_cursor:
  #pprint.pprint(executionid["provenance"]["analysis_execution_id"]);
  execution_id=executionid["provenance"]["analysis_execution_id"]
  print execution_id
  execution_id_array.append(execution_id)
  
print  execution_id_array[0],execution_id_array[1]


for annotation in collection.find({"provenance.image.case_id":case_id,
              "provenance.image.subject_id":subject_id,
             "provenance.analysis.execution_id": { $in:[execution_id_array]}
            },{"_id":0,"geometry.coordinates":1}):pprint.pprint(annotation)