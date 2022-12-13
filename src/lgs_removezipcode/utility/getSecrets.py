import base64
import json

import boto3
from botocore.exceptions import ClientError


class SecretsManager:
    def __init__(self, secret_name):

        self.secret_name = secret_name
        self.region_name = "us-east-1"
        self.secret = None
        self.secret_dict = {}

    def get_secrets(self) -> str:

        secret_name = self.secret_name
        region_name = self.region_name

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.

            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]
                self.secret = secret
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                )

    # @property
    def transform_string(self, db_string: bool = False) -> dict:
        """transforms AWS SecretString from get_secret() into SQLAlchemy engine-ready format"""
        self.get_secrets()
        try:
            secret_dict = json.loads(self.secret)
        except Exception as e:
            raise e

        if db_string:
            required_keys: set = {"username", "password", "host", "port", "dbname"}

            if required_keys <= secret_dict.keys():
                self.secret_dict = secret_dict
            else:
                raise Exception("Required Keys not found in secrets dictionary")
        else:
            self.secret_dict = secret_dict

        return self.secret_dict


if __name__ == "__main__":
    s = SecretsManager()
    s.transform_string()
