import json
import boto3
import botocore
import os

def lambda_handler(event, context):
    
    bucket=os.environ['S3_BUCKET_NAME']
    region=os.environ['S3_REGION']
    document=event['queryStringParameters']['file']
    
    #S3 bucket is in a different region, hence. the need to specify region when creating boto3 client
    textractClient = boto3.client('textract', region_name=region)
    s3 = boto3.resource('s3', region_name=region)
    
    # check if object exists in bucket
    try:
        s3.Object(bucket, document).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return {
                'statusCode': 404,
                'body': 'Invalid file name in URL query param'
            }

    #process using S3 object
    response = textractClient.analyze_document(Document={'S3Object': {'Bucket': bucket, 'Name': document}}, FeatureTypes=['FORMS'])

    #Get the text blocks
    blocks=response['Blocks']
    
    #Get the key-value map
    key_map, value_map, block_map = get_kv_map(blocks)
    
    #Get the key-value relationship
    kvs = get_kv_relationship(key_map, value_map, block_map)
    
    return {
        'statusCode': 200,
        'body': json.dumps(kvs)
    }

def get_kv_map(blocks):

    # get key and value maps
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    return key_map, value_map, block_map

def get_kv_relationship(key_map, value_map, block_map):
    kvs = {}
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs[key] = val
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '    

                                
    return text
