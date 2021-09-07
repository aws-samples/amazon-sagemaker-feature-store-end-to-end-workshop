## Automate feature engineering pipelines with Amazon SageMaker

### Overview
In this repository, we provide artifacts that demonstrate how to leverage Amazon SageMaker Data Wrangler, Amazon SageMaker Feature Store, and Amazon SageMaker Pipelines alongside AWS Lambda to automate feature transformation. The instructions in this notebook are meant to be performed following those in module 05 (`05-module-scalable-batch-ingestion`) of this training, which walks through the creation of a Data Wrangler Flow file. We will reference, load, and modify the flow file created by the notebook here:
[notebook](https://github.com/arunprsh/feature-store-champions-workshop/blob/main/05-module-scalable-batch-ingestion/3_sm_data_wrangler.ipynb)

Our objective is to configure a simple workflow using SageMaker Pipelines that runs feature transformations using SageMaker Data Wrangler and exports the resulting data to SageMaker Feature Store. We use an AWS Lambda function and S3 event to trigger this automated workflow. The architecture for enabling this workflow is shown in the diagram below.

![process overview](./images/process-overview.png)

### Dataset
To demonstrate feature pipeline automation, we use the simulated dataset generated in module 01 (`01-module-feature-store-foundations`) of this repository here: 
[notebook](https://github.com/arunprsh/feature-store-champions-workshop/blob/main/01-module-feature-store-foundations/0_prepare_datasets.ipynb)

The datasets created include a sample of 100,000 synthetic grocery orders from a total of 10,000 synthetically generated customers list. For each customer, the notebook generates between 1 to 10 of their orders, with products purchased in each order. The notebook also generates a timestamp on which the order was placed.

### Prerequisites
- An [AWS account](https://portal.aws.amazon.com/billing/signup/resume&client_id=signup)
- An Amazon [SageMaker Studio domain](https://docs.aws.amazon.com/sagemaker/latest/dg/onboard-quick-start.html) with the `AmazonSageMakerFeatureStoreAccess` managed policy [attached to the IAM execution role](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_manage-attach-detach.html#add-policies-console)
- An [Amazon S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)

### Complete Walkthrough
For a full walkthrough of automating feature transformation with Amazon SageMaker, see this [blog post](https://aws.amazon.com/blogs/machine-learning/automate-feature-engineering-pipelines-with-amazon-sagemaker/). It explains more about how to use Amazon SageMaker Data Wrangler for feature transformation, Amazon SageMaker Feature Store for storing those features and Amazon SageMaker Pipelines for automating transformations of all future data.

For a general overview on how to use Amazon SageMaker Data Wrangler, please refer to [developer documentation](https://docs.aws.amazon.com/sagemaker/latest/dg/data-wrangler-getting-started.html).

### Original Notebook
This notebook is an extension built upon the notebook example in this repository:
[notebook](https://github.com/aws-samples/amazon-sagemaker-automated-feature-transformation)

To follow along, download the Jupyter notebook and python code from the referenced repo. All the instructions are in the blog post and the Jupyter notebook. 

### Clean-up 

To avoid any recurring charges, stop any running Data Wrangler and Jupyter Notebook instances within Studio when not in use. 


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

