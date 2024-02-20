
    import json
    import boto3
    import datetime

    s3 = boto3.resource('s3')
    sm = boto3.client('sagemaker')
    time_created = datetime.datetime.now()

    def lambda_handler(event, context):

        print(f'Time Lambda created: {time_created}')

        #Check version of Boto3 - It must be at least 1.16.55
        print(f"The version of Boto3 is {boto3.__version__}")

        #Get location for where the new data (csv) file was uploaded
        data_bucket = event['Records'][0]['s3']['bucket']['name']
        data_key = event['Records'][0]['s3']['object']['key']
        print(f"A new file named {data_key} was just uploaded to Amazon S3 in {data_bucket}")

        #Update values for where Data Wrangler .flow is saved
        flow_bucket = 'sagemaker-us-east-1-572539092864'
        flow_key = 'sagemaker-feature-store/fscw/data_wrangler_flows/DWF-Orders.flow'
        pipeline_name = 'featurestore-ingest-pipeline-20-18-41-16'
        execution_display = f"{data_key.split('/')[-1].replace('_','').replace('.csv','')}"


        #Get .flow file from Amazon S3
        get_object = s3.Object(flow_bucket,flow_key)
        get_flow = get_object.get()

        #Read, update and save the .flow file
        flow_content = json.loads(get_flow['Body'].read())
        flow_content['nodes'][0]['parameters']['dataset_definition']['name'] = data_key.split('/')[-1]
        flow_content['nodes'][0]['parameters']['dataset_definition']['s3ExecutionContext']['s3Uri'] = f"s3://{data_bucket}/{data_key}"
        new_flow_key = flow_key.replace('.flow', '-' + data_key.split('/')[-1].replace('.csv','') + '.flow')
        new_flow_uri = f"s3://{flow_bucket}/{new_flow_key}"
        put_object = s3.Object(flow_bucket,new_flow_key)
        put_flow = put_object.put(Body=json.dumps(flow_content))


        #Start the pipeline execution
        start_pipeline = sm.start_pipeline_execution(
                        PipelineName=pipeline_name,
                        PipelineExecutionDisplayName=f"{data_key.split('/')[-1].replace('_','').replace('.csv','')}",
                        PipelineParameters=[
                            {
                                'Name': 'InputFlow',
                                'Value': new_flow_uri
                            },
                        ],
                        PipelineExecutionDescription=data_key
                        )
        print(start_pipeline)


        return('SageMaker Pipeline has been successfully started...')
    