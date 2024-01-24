## SageMaker Feature Store Champions Workshop - Module 10

In this module we cover how to implement security controls for feature store

### 10-hard-delete
In this sub-module, we provide an approach of how to erase data from the online and offline feature store. We will talk about the concept of “hard-delete” in Iceberg which is removing the data entirely from feature store as opposed to "soft-delete" where we are flagging records as deleted without physically removing them from storage.

* Setup of Feature Group in Iceberg table format
* Hard Delete records using DeleteRecord API
* Hard Delete records from Offline Store with Iceberg Compaction procedures

#### Notebooks:
* m10_nb1_offline_hard_delete.ipynb
