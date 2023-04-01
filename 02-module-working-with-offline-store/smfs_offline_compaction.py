import boto3
import time
import sys
import awsglue
from awsglue.utils import getResolvedOptions

def run_vacuum_query(athena, database, table, work_group, output_location):
    
    vacuum_sql = f"vacuum {database}.{table}"

    fully_completed = False
    while not fully_completed:
        print(f"executing {vacuum_sql}")
        execution = athena.start_query_execution(
            QueryString=vacuum_sql, 
            WorkGroup=work_group,
            ResultConfiguration={
            'OutputLocation': output_location,
            }
        )
        print("Debugging: After executing Athena query")
        result = None
        status = 'RUNNING'
        id = execution['QueryExecutionId']
        while status == 'RUNNING' or status == 'QUEUED':
            print(f"waiting {id} for {wait_sleep_seconds} seconds")
            time.sleep(wait_sleep_seconds)
            result = athena.get_query_execution(QueryExecutionId=id)
            status = result['QueryExecution']['Status']['State']
        print(f"Debugging, status: {status} and result:")
        print(result)

        if status == 'SUCCEEDED':
            fully_completed = True
            print(f"Fully completed vacuum")
        elif status == 'FAILED':
            error = result['QueryExecution']['Status']['StateChangeReason']
            raise Exception(f"Unexpected error: {error}, execution ID: {id}")
        else:
            raise Exception(f"Unexpected status: {status}, execution ID: {id}")
            

args = getResolvedOptions(sys.argv,
                          ['region',
                           'database',
                           'workgroup',
                           'outputlocation',
                           'table'])

print ("region: ", args['region'])
print ("database: ", args['database'])
print ("table: ", args['table'])
print ("workgroup: ", args['workgroup'])
print ("outputlocation: ", args['outputlocation'])

# assume v3 and result output location is pre-configured
region_name = args['region']
work_group = args['workgroup']
database = args['database']
table = args['table']
output_location = args['outputlocation']

optimize_sql = f"optimize {database}.{table} rewrite data using bin_pack"
wait_sleep_seconds = 10

athena = boto3.client("athena", region_name=region_name)
fully_completed = False
counter = 0
while not fully_completed:
    counter += 1
    print(f"executing {optimize_sql} round {counter}")
    execution = athena.start_query_execution(
        QueryString=optimize_sql, 
        WorkGroup=work_group,
        ResultConfiguration={
        'OutputLocation': output_location,
        }
    )
    print("Debugging: After executing Athena query")
    result = None
    status = 'RUNNING'
    id = execution['QueryExecutionId']
    while status == 'RUNNING' or status == 'QUEUED':
        print(f"waiting {id} for {wait_sleep_seconds} seconds")
        time.sleep(wait_sleep_seconds)
        result = athena.get_query_execution(QueryExecutionId=id)
        status = result['QueryExecution']['Status']['State']
    print(f"Debugging, status: {status} and result:")
    print(result)
    
    if status == 'SUCCEEDED':
        fully_completed = True
        print(f"Fully completed compaction, took {counter} rounds")
        run_vacuum_query(athena, database, table, work_group, output_location)
    elif status == 'FAILED':
        error = result['QueryExecution']['Status']['StateChangeReason']
        if "ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED" in error:
            print(f"Compaction not fully completed, start another run.")
        else:
            raise Exception(f"Unexpected error: {error}, execution ID: {id}")
    else:
        raise Exception(f"Unexpected status: {status}, execution ID: {id}")


