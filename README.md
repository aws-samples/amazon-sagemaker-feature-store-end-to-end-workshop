## SageMaker Feature Store Workshop 

![workshop](./images/workshop.png)

* **Module 1: Feature Store Foundations**
    * **Topics:**
        * Dataset introduction
        * Creating a feature group
        * Ingesting a Pandas DataFrame into Online/Offline feature store
        * GetRecord, ListFeatureGroups, DescribeFeatureGroup

* **Module 2: Working with the Offline Store**
    * **Topics:**
        * Look at data in S3 console (Offline feature store)
        * Athena query for dataset extraction (via Athena console)
        * Athena query for dataset extraction (programmatically using SageMaker SDK)
        * Extract a training dataset and storing in S3
        
* **Module 3: Training a model using extracted dataset from the Offline feature store**
    * **Topics:**
        * Training a model using feature sets derived from the Offline feature store
        * Deploying the trained model for real-time inference
        
* **Module 4: Leveraging the Online feature store**
    * **Topics:**
        * Get record from Online feature store during single inference
        * Get multiple records from Online store using BatchGet during batch inference 

* **Module 5: Scalable batch ingestion using distributed processing**
    * **Topics:**
        * Batch ingestion via SageMaker Processing job
        * Batch ingestion via SageMaker Processing PySpark job
        * SageMaker Data Wrangler export job to feature store
* **Module 6: Automate feature engineering pipelines with Amazon SageMaker**
    * **Topics:**
       * Leverage Amazon SageMaker Data Wrangler, Amazon SageMaker Feature Store, and Amazon SageMaker Pipelines alongside AWS Lambda to automate feature transformation.

* **Module 7 : Feature Monitoring**
    * **Topics:**
       * Feature Group Monitoring Preparation, DataBrew Dataset Creation
       * Run Feature Group Monitoring using DataBrew Profile Job
       * Visualization of Feature Group Statistics and Feature Drift
