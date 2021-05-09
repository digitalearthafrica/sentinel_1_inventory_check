import boto3
from botocore.config import Config
import pandas as pd

SOURCE_BUCKET = "deafrica-sentinel-1-staging-frankfurt"
FILE_NAME = os.getenv("FILE_NAME","missing-data.txt")

my_config = Config(region_name='eu-central-1')
s3 = boto3.client("s3", config=my_config)
s3r = boto3.resource('s3', config=my_config)
bucket = s3r.Bucket(SOURCE_BUCKET)

# TODO: Improvement - To process large csv file, use multithreading approach.
paths = set(pd.read_csv(FILE_NAME).path)

for path in paths:
    print(f"Working on {path}")
    resp = s3.list_object_versions(Bucket=SOURCE_BUCKET, Prefix=path)
    try:
        # All delete markers that are latest version, i.e., should be deleted
        del_markers = {item['Key']: item['VersionId'] for item in resp['DeleteMarkers'] if item['IsLatest'] == True}

        for key, version in del_markers.items():
            object = bucket.Object(key)
            object.delete(VersionId=version)

            head = s3.head_object(Bucket=SOURCE_BUCKET, Key=key)
            replication_status = head.get("ReplicationStatus")

            if replication_status and replication_status != "COMPLETED":
                print(f"Replicating {key}")

                s3r.Object(SOURCE_BUCKET, key).copy_from(
                    CopySource=f'{SOURCE_BUCKET}/{key}',
                    MetadataDirective='REPLACE'
                )
            else:
                print(f"Key is replicated: {key}")
    except KeyError:
        print(f"Keys has no DeleteMarkers for: {path}")
        pass