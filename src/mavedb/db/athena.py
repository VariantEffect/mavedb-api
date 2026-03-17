import os
from urllib.parse import quote_plus

from sqlalchemy.engine import create_engine

AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-west-2")
ATHENA_SCHEMA_NAME = os.getenv("ATHENA_SCHEMA_NAME", "default")
ATHENA_S3_STAGING_DIR = os.getenv("ATHENA_S3_STAGING_DIR")
ATHENA_AWS_ACCESS_KEY = os.getenv("ATHENA_AWS_ACCESS_KEY", "")
ATHENA_AWS_ACCESS_KEY_SECRET = os.getenv("ATHENA_AWS_ACCESS_KEY_SECRET", "")

ATHENA_URL = "awsathena+rest://{access_key}:{access_key_secret}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"

engine = create_engine(
    ATHENA_URL.format(
        region_name=AWS_REGION_NAME,
        schema_name=ATHENA_SCHEMA_NAME,
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
        access_key=quote_plus(ATHENA_AWS_ACCESS_KEY),
        access_key_secret=quote_plus(ATHENA_AWS_ACCESS_KEY_SECRET)
    )
)
