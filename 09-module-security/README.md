## SageMaker Feature Store Champions Workshop - Module 9

In this module we cover how to implement security controls for feature store

### 09-01-granular-access-control-lake-formation-offline
In this sub-module, we provide an approach of how to implement granular access control to feature groups and features stored in an offline feature store using Amazon SageMaker Feature Store and AWS Lake Formation.  The approach uses AWS Lake Formation to implement row, column or cell level access to limit which feature groups or which features within a feature group can be accessed by a data scientist working in Amazon SageMaker Studio. While we will focus on restricting access to users working in SageMaker Studio, the same approach is applicable for users accessing the offline feature store using services like Amazon Athena.

* Setup of granular access control to Offline Feature Store using AWS Lake Formation
* Testing of the access control using SageMaker Studio
* Clean up the access control in Lake Formation

#### Notebooks:
* m9_01_nb0_row_cell_level_access_lf_setup.ipynb
* m9_01_nb1_row_cell_level_access_test.ipynb
* m9_01_nb2_row_cell_level_access_clean_up.ipynb


### 09-02-fg-access-control-iam-policy-online-offline
In this sub-module, we provide an approach of how to implement access control for feature groups in an online and offline feature store using Amazon SageMaker and IAM role and policies.

#### Notebooks:
* m9_02_nb1_iam_policy_online_offline.ipynb