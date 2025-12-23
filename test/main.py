#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab
import os
import sys
import json
import time
import datetime
import random
import boto3
import uuid
import argparse

from datasets import load_dataset
from botocore.exceptions import ClientError

def format_datetime(fmt: str) -> str:
    CURRENT_YEAR = datetime.datetime.now().year
    CURRENT_MONTH = datetime.datetime.now().month
    CURRENT_DAY = datetime.datetime.now().day
    CURRENT_HOUR = datetime.datetime.now().hour
    CURRENT_MINUTE = datetime.datetime.now().minute
    CURRENT_SECOND = datetime.datetime.now().second

    return datetime.datetime.combine(
      date=datetime.date(CURRENT_YEAR, CURRENT_MONTH, CURRENT_DAY),
      time=datetime.time(CURRENT_HOUR, CURRENT_MINUTE, CURRENT_SECOND)
    ).strftime(fmt)


def main():
    # Parse command arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", type=int, help="The number of events to ingest.")
    parser.add_argument("-s", "--stream", type=str, help="The name of the Kinesis Stream.")
    parser.add_argument("-r", "--region", type=str, default="us-east-1", help="The AWS Region.")
    args = parser.parse_args()

    # Download News Data
    print("Downloading Sample News Event Data ...")
    dataset = load_dataset("abisee/cnn_dailymail", "3.0.0", split="train")
    dataset = dataset.shuffle()
    dataset = dataset.select(range(int(args.count)))

    # Put news event records into Kinesis
    client = boto3.client("kinesis", region_name=args.region)
    cnt = 0
    for record in dataset:
        try:
            data = {
                "event_id": record["id"],
                "updated_at": format_datetime(fmt="%Y-%m-%d %H:%M:%S"),
                "summary": record["highlights"],
                "event": record["article"]
            }
            response = client.put_record(
                StreamName=args.stream,
                Data=f"{json.dumps(data)}\n", # JSON lines format
                PartitionKey=str(uuid.uuid4())[0:8]
            )
            # print(f"[RESPONSE] {response}")
            # print(f"[RESPONSE] {response["ResponseMetadata"]["HTTPStatusCode"]}")
        
        except ClientError as e:
            message = e.response["Error"]["Message"]
            print(message, file=sys.stderr)
        if cnt % 100 == 0:
            print(f'[INFO] {cnt} records are processed', file=sys.stderr)
        time.sleep(random.choices([0.1, 0.3, 0.5, 0.7])[-1])
        cnt += 1
    print(f'[INFO] Total {cnt} records are processed', file=sys.stderr)


if __name__ == "__main__":
    main()
