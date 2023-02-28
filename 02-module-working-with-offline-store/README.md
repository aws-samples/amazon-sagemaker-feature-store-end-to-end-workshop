## SageMaker Feature Store Champions Workshop - Module 2 

### Working with Offline store 
* Look at data in S3 console 
* Athena query for dataset extraction (via Athena console)
* Athena query for dataset extraction (programmatically in SageMaker notebooks)
* Extract a training dataset and storing in S3
* Use row-level time travel utility to easily extract point-in-time correct data given a dataframe of events

#### Notebook:
* m2_nb1_feature_store_dataset_extraction.ipynb

### Search and Discovery using Feature-Level Metadata
* Retrieve feature group
* Update feature's metadata (description and parameters)
* Search for feature using its custom metadata (using Boto3 API and Amazon SageMaker Studio) 

#### Notebook:
* m2_nb2_feature_metadata_search_discovery.ipynb

### Apache Iceberg offline store compaction
* Create Feature Groups in Iceberg format
* Offline store compaction using Amazon Athena
* Offline store compaction using Spark
* Scheduled compaction

#### Notebook:
* m2_nb3_offline_iceberg_compaction.ipynb

