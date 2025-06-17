import os

from sqlalchemy.engine import create_engine

AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-west-2")
ATHENA_SCHEMA_NAME = os.getenv("ATHENA_SCHEMA_NAME", "default")
ATHENA_S3_STAGING_DIR = os.getenv("ATHENA_S3_STAGING_DIR")

ATHENA_URL = "awsathena+rest://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"

GNOMAD_JOINT_FREQUENCY_TABLE_NAME = os.getenv("GNOMAD_JOINT_FREQUENCY_TABLE_NAME", "mavedb2_gnomad_data")

engine = create_engine(
    ATHENA_URL.format(
        region_name=AWS_REGION_NAME,
        schema_name=ATHENA_SCHEMA_NAME,
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
    )
)
