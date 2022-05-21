from contextlib import nullcontext
from datetime import datetime
from operator import truediv
from urllib import response
import boto3
from io import BytesIO
from PIL import Image, ImageOps
import os
import uuid
import json


s3 = boto3.client("s3")
size = int(os.environ['THUMBNAIL_SIZE'])
db_table = str(os.environ['DYNAMODB_TABLE'])
dynamodb = boto3.resource('dynamodb', region_name=str(os.environ['REGION_NAME']))

def s3_thumbnail_generator(event, context):
    # parse event
    print("EVENT:::", event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    img_size = event['Records'][0]['s3']['object']['size']

    if (not key.endswith("_thumbnail.png")):
        image = get_s3_image(bucket, key)
        
        thumbnail = image_to_thumbnail(image)

        thumbnail_key = new_filename(key)

        url = upload_to_s3(bucket, thumbnail_key, thumbnail, img_size)

        return url


def get_s3_image(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    imageContent = response['Body'].read()

    file = BytesIO(imageContent)
    img = Image.open(file)
    return img

def image_to_thumbnail(image):
    return ImageOps.fit(image, (size, size), Image.ANTIALIAS)

def new_filename(key):
    key_split = key.rsplit('.', 1)
    new_key = key_split[0] + "_thumbnail.png"
    print(new_key)
    return new_key

def upload_to_s3(bucket, key, image, img_size):
    out_thumbnail = BytesIO()
    image.save(out_thumbnail, 'PNG')
    out_thumbnail.seek(0)

    response = s3.put_object(
        ACL = 'public-read',
        Body = out_thumbnail,
        Bucket = bucket,
        ContentType='image/png',
        Key=key
    )
    print(response)

    url = '{}/{}/{}'.format(s3.meta.endpoint_url, bucket, key)
    print(url)

    s3_save_thumbnail_url_to_dynamo(url_path=url, img_size=img_size)
    return url

def s3_save_thumbnail_url_to_dynamo(url_path, img_size):
    to_int = float(img_size * 0.53)/1000
    table = dynamodb.Table(db_table)
    response = table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'url': str(url_path),
            'approxReducedSize': str(to_int) + str(' KB'),
            'createdAt': str(datetime.now()),
            'updatedAt': str(datetime.now())
        }
    )

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response)
    }


def s3_get_thumbnail_urls(event, context):
    table = dynamodb.Table(db_table) 
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(data)
    }

def s3_get_item(event, context):

    table = dynamodb.Table(db_table)
    print(event)
    response = table.get_item(Key={'id': event['pathParameters']['id']})
    print(response)

    item = response['Item']

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(item),
        'isBase64Encoded': False
    }

def s3_delete_item(event, context):
    item_id = event['pathParameters']['id']

    table = dynamodb.Table(db_table)
    response = table.delete_item(Key={'id': event['pathParameters']['id']})
    
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:        
        delete_response = {
            "deleted": True,
            "itemDeletedId": item_id
        }
    else:
        delete_response = {
            "statusCode": 500,
            "body": f"An error occurred while deleting post {item_id}"
        }

    print(delete_response)
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(delete_response),
        'isBase64Encoded': False
    }
