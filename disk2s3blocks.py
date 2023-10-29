#!/usr/bin/python3

import os
import sys
import time
import argparse
import subprocess
import hashlib
import base64
import multiprocessing
import zlib
import json
import math
import shutil
import boto3
import botocore
import tqdm
import requests

BLOCK_SIZE = 15728640  # 15MB


def get_disk_size(disk_path):
    process = subprocess.run(['/usr/sbin/blockdev', '--getsize64', disk_path], capture_output=True)
    return int(process.stdout)

SESSION = boto3.session.Session()
CLIENT = boto3.client(
    service_name='s3',
    config=botocore.config.Config(
        s3={
            'use_accelerate_endpoint': True
        }
    )
)
BUCKET = 'andychweb-uploads'
PREFIX = 'disks'
S3_CHECKSUMS = {}
LOCAL_CHECKSUMS = {}


def block_exists_s3(s3_name, block_pos):
    block_id = int(block_pos / BLOCK_SIZE)
    try:
        response = CLIENT.head_object(
            Bucket=BUCKET,
            Key=f'{PREFIX}/{s3_name}/block_{block_id}.bin.gz'
        )
    except botocore.exceptions.ClientError:
        return False
    S3_CHECKSUMS[block_id] = response['Metadata']['uncompressedsha1']
    return True


def fetch_block(disk_path, block_pos):
    with open(disk_path, 'rb') as fo:
        fo.seek(block_pos)
        return fo.read(BLOCK_SIZE)


def get_data_hash(data):
    hash_object = hashlib.sha1(data)
    pb_hash = hash_object.digest()
    return base64.b64encode(pb_hash).decode('utf8')


def get_block_hash(disk_path, block_pos):
    block_id = int(block_pos / BLOCK_SIZE)
    if block_id in LOCAL_CHECKSUMS:
        return LOCAL_CHECKSUMS[block_id]
    block_contents = fetch_block(disk_path, block_pos)
    base64_encoded = get_data_hash(block_contents)
    LOCAL_CHECKSUMS[block_id] = base64_encoded
    return base64_encoded


def block_changed(disk_path, s3_name, block_pos):
    block_id = int(block_pos / BLOCK_SIZE)
    if block_id not in S3_CHECKSUMS:
        if not block_exists_s3(s3_name, block_pos):
            return True
    return get_block_hash(disk_path, block_pos) != S3_CHECKSUMS[block_id]


def upload_block(disk_path, s3_name, block_pos, compression):
    start_time = time.time()
    block_id = int(block_pos / BLOCK_SIZE)
    block_contents = fetch_block(disk_path, block_pos)
    compressed = zlib.compress(block_contents, level=compression)
    response = CLIENT.put_object(
        Body=compressed,
        Bucket=BUCKET,
        ChecksumSHA1=get_data_hash(compressed),
        Metadata={
            'uncompressedsha1': get_block_hash(disk_path, block_pos)
        },
        Key=f'{PREFIX}/{s3_name}/block_{block_id}.bin.gz'
    )
    return time.time() - start_time


def is_block_needs_upload(disk_path, s3_name, block_pos):
    if block_exists_s3(s3_name, block_pos):
        if block_changed(disk_path, s3_name, block_pos):
            return True
        return False
    return True


def get_blocks_to_upload(disk_path, s3_name):
    print('Checking blocks upload status and checksums...')
    with multiprocessing.Pool() as pool:
        async_results = []
        for block_pos in range(0, get_disk_size(disk_path), BLOCK_SIZE):
            async_results.append(pool.apply_async(is_block_needs_upload, (disk_path, s3_name, block_pos)))
        results = []
        for i, async_result in tqdm.tqdm(list(enumerate(async_results))):
            if async_result.get():
                results.append(i * BLOCK_SIZE)
        return results


def async_upload_blocks(disk_path, s3_name, blocks_to_upload, compression):
    if not blocks_to_upload:
        return 0
    with multiprocessing.Pool() as pool:
    #for block_pos in tqdm.tqdm(blocks_to_upload):  ETA 3 hours for 10GB disk
    # Pool with 10 workers - ETA 3 hours for 10GB disk
    # Pool with 5 workers - ETA 2:22 for 10GB disk
    # Pool with 8 workers with compression - 1 hour for 10GB disk
        async_results = []
        for block_pos in blocks_to_upload:
            async_results.append(pool.apply_async(upload_block, (disk_path, s3_name, block_pos, compression)))
        results = []
        for async_result in tqdm.tqdm(async_results):
            results.append(async_result.get())
        return sum(results) / len(results)


def upload_metadata(disk_path, s3_name):
    metadata = {
        'block_size': BLOCK_SIZE,
        'blocks_num': math.ceil(get_disk_size(disk_path) / BLOCK_SIZE)
    }
    response = CLIENT.put_object(
        Body=json.dumps(metadata),
        Bucket=BUCKET,
        Key=f'{PREFIX}/{s3_name}/block_metadata.json'
    )


def process_blocks(disk_path, s3_name):
    if os.getuid() != 0:  # If not root
        print('ERROR: This program must be run from superuser')
        sys.exit(1)
    upload_metadata(disk_path, s3_name)
    blocks_to_upload = get_blocks_to_upload(disk_path, s3_name)
    print(f'{len(blocks_to_upload)} blocks need uploading. Starting now...')
    print('Estimating optimal compression factor:')
    upload_stats = []
    for compression in range(10):
        upload_time = async_upload_blocks(disk_path, s3_name, blocks_to_upload[compression * 2:(compression + 1) * 2], compression)
        print(f'Upload time for compression factor {compression}: {upload_time}')
        upload_stats.append(upload_time)
    best_compression = upload_stats.index(min(upload_stats))
    print(f'Best compression factor is {best_compression}. Will use it for next uploads.')
    async_upload_blocks(disk_path, s3_name, blocks_to_upload[20:], best_compression)
    print('Upload complete.')


def zerofill_partition(partition):
    print(f'Zerofilling free space on partition {partition}...')
    mount_dir = 'zerofill_mnt'
    zerofile = f'zerofill_mnt/zerofill{partition.replace("/", "_")}'
    os.makedirs(mount_dir, exist_ok=True)
    subprocess.run(['/usr/bin/mount', partition, mount_dir], check=True)
    free_space = shutil.disk_usage(mount_dir).free
    with open(zerofile, 'wb') as fo:
        for _ in tqdm.tqdm(range(free_space // BLOCK_SIZE)):
            fo.write(b'\x00' * BLOCK_SIZE)
        for _ in range(free_space % BLOCK_SIZE - 1):
            fo.write(b'\x00')
    os.remove(zerofile)
    subprocess.run(['/usr/bin/umount', mount_dir], check=True)


def zerofill_disk(disk_path):
    # TODO: consider using shred, ntfsclone
    process = subprocess.run(['/usr/sbin/fdisk', '-l', disk_path], capture_output=True, check=True)
    print(process.stdout.decode('utf8'))
    print('Zerofilling the disk is generally not recommended for SSD disks')
    question = f'Would you like to zerofill each partition of {disk_path}? (y/n): '
    if input(question).lower() != 'y':
        return
    print(f'Checking partitions of disk {disk_path}...')
    for line in process.stdout.decode('utf8').split('\n'):
        if not line.startswith(disk_path):
            continue
        partition = line.split(' ')[0]
        zerofill_partition(partition)


def prepare_telegram_token():
    if os.path.exists('telegram.json'):
        return
    config = {
        'token': input('Please enter Telegram bot token: '),
        'chat_id': input('Please enter Telegram chat id: ')
    }
    with open('telegram.json', 'w') as fo:
        json.dump(config, fo)


def send_telegram(message):
    with open('telegram.json') as fo:
        config = json.load(fo)
    url = f"https://api.telegram.org/bot{config['token']}/sendMessage"
    payload = {
        "text": message.encode("utf8"),
        "chat_id": config['chat_id']
    }
    requests.post(url, payload)


def main():
    parser = argparse.ArgumentParser(
        prog='./disk2s3blocks.py',
        description='Captures and uploads a disk image to S3, minding the changed blocks'
    )
    parser.add_argument('disk_path', help='Local path to the block device, e.g. /dev/sda')
    parser.add_argument('s3_name', help='Disk image name in S3')
    args = parser.parse_args()
    prepare_telegram_token()
    try:
        zerofill_disk(args.disk_path)
        process_blocks(args.disk_path, args.s3_name)
        send_telegram(f'{args.s3_name} disk upload complete')
    except Exception as exc:
        send_telegram(f'Exception during {args.s3_name} disk upload:\n{exc}')
        raise exc


if __name__ == '__main__':
    main()
