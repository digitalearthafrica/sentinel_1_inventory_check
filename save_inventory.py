#!/usr/bin/env python3

import csv
import gzip
import json
import os
import sys

import boto3

s3 = boto3.resource("s3")

bucket = "deafrica-sentinel-1-inventory"
manifest_key = "deafrica-sentinel-1/deafrica-sentinel-1-inventory/2021-05-05T00-00Z/manifest.json"

print("Starting up...")


def log(comment):
    sys.stdout.write(f"\r{comment}")


# Stolen from https://alukach.com/posts/parsing-s3-inventory-output
def list_keys(bucket, manifest_key):
    manifest = json.load(s3.Object(bucket, manifest_key).get()["Body"])
    for item in manifest["files"]:
        gzip_obj = s3.Object(bucket_name=bucket, key=item["key"])
        buffer = gzip.open(gzip_obj.get()["Body"], mode="rt")
        reader = csv.reader(buffer)
        for row in reader:
            yield row

limit = 2
count = 0
log_every = 10000

if __name__ == "__main__":
    with gzip.open("sentinel-1.list.gz", "wt") as text_file:
        for bucket, key, *rest in list_keys(bucket, manifest_key):
            if count % log_every == 0:
                log(f"Found {count} scenes...")
            text_file.write(f"s3://{bucket}/{key}\n")
            count += 1

print(f"Found {count} files")
