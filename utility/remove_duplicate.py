db.metadata.aggregate(
    {
	$match : {"provenance.analysis_execution_id":{'$regex' : 'composite_dataset', '$options' : 'i'}}
    },
    {
	$group : { _id : "$image.case_id", total : { $sum : 1 } }
    }
  );
