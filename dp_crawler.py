import json
import boto3
import json
import os

temp_user_access_key = ""
temp_user_secert_key = ""
#local_path = "/Users/kenhung/Desktop/python/dynamic_pricing_hist_crawler/tmp/test.jsonl"
local_path = "/tmp/test.jsonl"

my_session = boto3.session.Session(
    aws_access_key_id=temp_user_access_key,
    aws_secret_access_key=temp_user_secert_key,
    region_name="us-west-2"
)
s3_client = my_session.client("s3")
s3_resource = my_session.resource('s3')

def json_crawler_decode(line):
    final_js = {}
    js = json.loads(line)

    '''
    Define json schema 
    '''

    final_js["request_id"] = js["request_id"]
    final_js["created_at"] = js["created_at"]
    final_js["optimized_at"] = js["optimized_at"]
    final_js["request"] = js["request"]
    final_js["result"] = js["result"]

    '''
    convert nested struct to struct with array
    '''
    ## issue key "inventory_status_history"
    #for n in range(len(result['content'])):
    inventory_status_history = final_js["request"]["inventory_status_history"]
    ## subsitute nested strict to list
    for i in range(len(inventory_status_history)):
        if isinstance(inventory_status_history[i], dict):
            indviual_list = [[j, inventory_status_history[i]['consumption'][j]] for j in inventory_status_history[i]['consumption']]
            ## subsititute to struct wrap with array
            final_js["request"]["inventory_status_history"][i]['consumption'] = indviual_list
        else:
            continue

    return final_js

'''
print(f"Downloading: {worksheet.title}")
    with open("/tmp/temp.csv", "w") as files:
        writer = csv.writer(files)
        writer.writerows(sheet_values)
'''
def json_crawler_encode(lines):
    with open(local_path, "w") as files:
        for line in lines:
            row = json.dumps(line)
            files.write(row+'\n')
    

def lambda_handler(event, context):  
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key_path = event["Records"][0]["s3"]["object"]["key"]
    result = s3_client.get_object(Bucket=bucket_name, Key=key_path)

    target_key_path = key_path.replace("price-opt", "price-opt-cleaned")

    cleaned_json_list = []
    
    texts = result["Body"].read().decode().splitlines()
    
    for line in texts:
        cleaned_file = json_crawler_decode(line)
        cleaned_json_list.append(cleaned_file)

    json_crawler_encode(cleaned_json_list)


    ## Write back to S3
    s3_resource.meta.client.upload_file(local_path, bucket_name, target_key_path)

    return {"result": 200, 
            "BuketName": bucket_name, 
            "key_path": key_path,
            "statement": "file cleaned"}


if __name__ == "__main__":

    # price-opt/2020/11/02/1604979533121033000389.jsonl
    # price-opt/2020/10/29/1604979523016695000135.jsonl
    # price-opt/2020/11/03/1604979535843158000554.jsonl
    # price-opt/2020/11/04/1605052818796458488501.jsonl
    s3_input_template = {
            "Records": [
                {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-west-2",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                    "name": "devgx-dp",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::example-bucket"
                    },
                    "object": {
                    "key": "price-opt/2020/11/04/1605052818796458488501.jsonl",
                    "size": 1024,
                    "eTag": "0123456789abcdef0123456789abcdef",
                    "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
                }
            ]}
    dummy_context = ""
    result = lambda_handler(s3_input_template, dummy_context)
    #print(result)

