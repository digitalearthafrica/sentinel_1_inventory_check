import boto3
from botocore.config import Config
import pandas as pd

SOURCE_BUCKET = "deafrica-sentinel-1-staging-frankfurt"

my_config = Config(region_name='eu-central-1')
s3 = boto3.client("s3", config=my_config)
s3r = boto3.resource('s3', config=my_config)
bucket = s3r.Bucket(SOURCE_BUCKET)

paths = set(pd.read_csv("missing-data.txt").path)

for path in paths:
    resp = s3.list_object_versions(Bucket=SOURCE_BUCKET, Prefix=path)
    del_markers = {item['Key']: item['VersionId'] for item in resp['DeleteMarkers'] if item['IsLatest'] == True}

    for key, version in del_markers.items():
        object = bucket.Object(key)
        object.delete(VersionId=version)

        s3r.Object(SOURCE_BUCKET, key).copy_from(
            CopySource=f'{SOURCE_BUCKET}/{key}',
            MetadataDirective='REPLACE'
        )
