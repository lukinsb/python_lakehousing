import json
from steps import extract, transform
import boto3
import os


if os.environ.get('env') == 'local':
    session = boto3.Session(profile_name='lucas', region_name='eu-west-1')
    path_to_save = os.getcwd()

else:
    os.chdir('/tmp')
    path_to_save = '/tmp'
    session = boto3.Session()


def lambda_handler(event, context):

    extract.extract_file(session)
    transform.transform_file(session)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "ETL Completed",
            # "location": ip.text.replace("\n", "")
        }),
    }
