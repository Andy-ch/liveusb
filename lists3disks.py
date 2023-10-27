#!/usr/bin/python3

import boto3

from disk2s3blocks import BUCKET, PREFIX

CLIENT = boto3.client('s3')


def main():
    response = CLIENT.list_objects_v2(
        Bucket=BUCKET,
        Delimiter='/',
        Prefix=PREFIX + '/'
    )
    is_truncated = response['IsTruncated']
    if is_truncated:
        cont_token = response['NextContinuationToken']
    print(response['CommonPrefixes'])
    while is_truncated:
        response = CLIENT.list_objects_v2(
            Bucket=BUCKET,
            Delimiter='/',
            Prefix=PREFIX + '/',
            ContinuationToken=cont_token
        )
        is_truncated = response['IsTruncated']
        if is_truncated:
            cont_token = response['NextContinuationToken']
        print(response['CommonPrefixes'])

if __name__ == '__main__':
    main()
