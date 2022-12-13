import boto3
import os
import pandas as pd
from extract_info import get_patient_detail, get_pharmacy_details
from utility.db import make_engine
from utility.utils import parse_envs, create_logger


os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')

env_name, environment_secrets = parse_envs()
print('Environment is:', env_name)
print('environment_secrets:', environment_secrets)
DB_ENV: str = environment_secrets["DB_ENV"]

SQS_REMOVED_ZIPCODE_INPUT_NM: str = environment_secrets["SQS_REMOVED_ZIPCODE_INPUT_NAME"]
SNS_REMOVED_ZIPCODE_OUTPUT_NM: str = environment_secrets["SNS_REMOVED_ZIPCODE_OUTPUT_NAME"]

logger = create_logger(logger_name="Removed-Patient-Zipcode", log_group_name="Removed-Patient-Zipcode-v1")

'''
        1. The below code extract the Input message published by Full stack team in Input SQS removed zipcode queue Exp MSG : {"pharmacy_id": "2", "zipcode": "07002"} and store to DF
        2. Pass the DF column pharmacy_id and zipcode to SQL and extract all matching records from Leads table.
        3. Push ALL the Output records to SNS topic Exp. MSG : {"PatientID": 2, "LOB": "MA","OperationType": "reassign"}
        3. Finally delete ALL processed message from Input SQS before Visibility timeout expire. 
'''

def sqs_message_extract():

    ''' Get ALL the messages exist in Input Removed Zipcode SQS'''

    pharmacy_df = pd.DataFrame(columns=['pharmacyid', 'zipcode'])
    receipt_list = []
    sqs_url = sqs_client.get_queue_url(QueueName=SQS_REMOVED_ZIPCODE_INPUT_NM)
    print("sqs_url", sqs_url['QueueUrl'])

    while True:
        response = sqs_client.receive_message(QueueUrl=sqs_url['QueueUrl'])

        if len(response.get('Messages', [])) == 0:
            break
        else:
            id, zipcode = get_pharmacy_details(response)
            pharmacy_df.loc[len(pharmacy_df.index)] = [id, zipcode]

            rec = response['Messages'][0]['ReceiptHandle']
            receipt_list.append(response['Messages'][0]['ReceiptHandle']) # Delete the SQS message before visibility timeout expire.
            sqs_client.change_message_visibility(QueueUrl=sqs_url['QueueUrl'], ReceiptHandle=rec, VisibilityTimeout=90)  #Set message visibility time 90 seconds

    return pharmacy_df, sqs_url['QueueUrl'], receipt_list


def create_topic():
    # Mock existing SNS output Topic to get topic-arn on-air
    print("Creating Mock topic...")
    response = sns_client.create_topic(Name=SNS_REMOVED_ZIPCODE_OUTPUT_NM)
    print("Created Mock topic successfully")

    return response['TopicArn']

def publish_out_msg(lead_df, topicarn):
    print("Publishing Removed Zipcode Output message to topic")

    logger.info(f"********Number of Lead record OperationType of reassign********** : {len(lead_df.index)}")
    for ind in lead_df.index:

        id = lead_df['patientid'][ind]
        lob = lead_df['lob'][ind]
        type = lead_df['operationtype'][ind]

        mesg = "{" + f'"patient_id": {id}, "LOB": "{lob}", "operation_type": "{type}"' + "}"
        #print("mesg :", mesg)
        message = sns_client.publish(TopicArn=topicarn, Message=mesg, MessageStructure='String')
    print("Message published Removed Zipcode output successfully")

def delete_sqs_message(sqs_url, receipt_list):

    for msg in receipt_list:
        delete_message = sqs_client.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg)
    print("Processed input message deleted from SQS")

def execute():
    try:
        logger.info("Starting Removed Zipcode Patient Zipcode process")

        # Receive all the message in Input SQS.
        message_df, sqs_urls, receipt_lst = sqs_message_extract()

        logger.info(f"*******Number on Input messages from SQS for processing********** : {len(receipt_lst)}")

        # Get all the Leads match with PharmacyId and Zipcode.
        engine = make_engine(db_env=DB_ENV)
        lead_data_df = get_patient_detail(engine=engine, pharmacy_df=message_df)

        #Mock Topic.
        topic_arn = create_topic()

        # Push the Leads records to Output SQS.
        publish_out_msg(lead_data_df, topic_arn)

        # Delete all the processes input message.
        delete_sqs_message(sqs_urls, receipt_lst)

    except Exception as e:
        logger.error(f"EXCEPTION! {e}")

if __name__ == "__main__":
    execute()
