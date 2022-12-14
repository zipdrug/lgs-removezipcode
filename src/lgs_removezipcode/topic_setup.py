import boto3
import os
from utility.utils import create_logger, parse_envs

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')

env_name, environment_secrets = parse_envs()
print('Environment is:', env_name)
print('environment_secrets:', environment_secrets)
DB_ENV: str = environment_secrets["DB_ENV"]
SQS_REMOVED_ZIPCODE_INPUT_NM: str = environment_secrets["SQS_REMOVED_ZIPCODE_INPUT_NAME"]
SNS_REMOVED_ZIPCODE_OUTPUT_NM: str = environment_secrets["SNS_REMOVED_ZIPCODE_OUTPUT_NAME"]

logger = create_logger(logger_name="Clone-Patient-Zipcode", log_group_name="Clone-Patient-Zipcode-v1")

'''
    One Time setup. The below code used to create any new SNS Topic,SQS Queue and Subscribe the Queue to Topic.
'''

def create_topic():
    print("Creating topic...")
    response = sns_client.create_topic(Name=SNS_REMOVED_ZIPCODE_OUTPUT_NM)
    print("Created topic successfully")

    return response['TopicArn']

def create_queue():
    print('Creating Queue')
    response = sqs_client.create_queue(QueueName=SQS_REMOVED_ZIPCODE_INPUT_NM)
    print('Queue created successfully')

    return response["QueueUrl"]

def sqs_arn(queue_url):
    response = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    #queue = sqs_client.get_queue_by_name(QueueName=SQS_REMOVED_ZIPCODE_INPUT_NM)
    print("queue:", response['Attributes']['QueueArn'])
    return response['Attributes']['QueueArn']

def subscribe_message(topic_arn,queue_arn):
    attribs = dict()
    attribs['RawMessageDelivery'] = 'True'
    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn, Attributes=attribs
    )
    print("Subscribe response:", response)
    return response

def execute():
    try:
        logger.info("Starting Topic Queue creation process")
        topic_arn = create_topic()
        queue_url = create_queue()
        queue_arn = sqs_arn(queue_url)
        subscribe_message(topic_arn, queue_arn)

    except Exception as e:
        logger.error(f"EXCEPTION! {e}")

if __name__ == "__main__":
    execute()
