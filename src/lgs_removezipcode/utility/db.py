from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import url
from utility.getSecrets import SecretsManager


def make_engine(db_env):
    zd_secrets = SecretsManager(secret_name=db_env)
    zd_secrets.transform_string(db_string=True)
    db_credentials = zd_secrets.secret_dict

    try:
        engine = create_engine(
            url.URL(
                drivername="postgresql+psycopg2",
                username=db_credentials["username"],
                password=db_credentials["password"],
                host=db_credentials["host"],
                database=db_credentials["dbname"],
            )
        )
    except Exception as e:
        raise e

    return engine
