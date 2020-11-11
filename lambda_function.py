import json
import boto3
import json
import os

local_path = "/tmp/test.jsonl"

my_session = boto3.session.Session(region_name="us-west-2")
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
            indviual_list = [[j, inventory_status_history[i]['consumption'][j]]
                             for j in inventory_status_history[i]['consumption']]
            ## subsititute to struct wrap with array
            final_js["request"]["inventory_status_history"][i]['consumption'] = indviual_list
        else:
            continue

    return final_js


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
    s3_resource.meta.client.upload_file(
        local_path, bucket_name, target_key_path)

    return {"result": 200,
            "BuketName": bucket_name,
            "key_path": key_path,
            "statement": "file cleaned"}