#!/usr/bin/python3

import os
import json
import gzip
import argparse
import multiprocessing
import tqdm
import boto3

from disk2s3blocks import get_block_hash

CLIENT = boto3.client('s3')
BUCKET = 'andychweb-uploads'
PREFIX = 'disks'
S3_CHECKSUMS = {}
LOCAL_CHECKSUMS = {}


def download_metadata(s3_name):
    response = CLIENT.get_object(
        Bucket=BUCKET,
        Key=f'{PREFIX}/{s3_name}/block_metadata.json'
    )
    return json.loads(response['Body'].read())


def is_block_exists_local(disk_path, block_pos):
    if os.path.exists(disk_path):
        return os.path.getsize(disk_path) > block_pos
    return False


def is_block_changed(s3_name, disk_path, block_pos, blocks_num):
    block_id = int(block_pos / blocks_num)
    if not is_block_exists_local(disk_path, block_pos):
        return True
    response = CLIENT.head_object(
        Bucket=BUCKET,
        Key=f'{PREFIX}/{s3_name}/block_{block_id}.bin.gz'
    )
    s3_checksum = response['Metadata']['uncompressedsha1']
    return s3_checksum != get_block_hash(disk_path, block_pos)


def is_block_download_needed(s3_name, disk_path, block_pos, blocks_num):
    if is_block_exists_local(disk_path, block_pos):
        return is_block_changed(s3_name, disk_path, block_pos, blocks_num)
    return True


def get_blocks_to_download(s3_name, disk_path):
    print('Checking blocks download status and checksums...')
    metadata = download_metadata(s3_name)
    block_size = metadata['block_size']
    blocks_num = metadata['blocks_num']
    with multiprocessing.Pool() as pool:
        async_results = []
        for block_pos in range(0, blocks_num * block_size, block_size):
            async_results.append(pool.apply_async(is_block_download_needed, (s3_name, disk_path, block_pos, blocks_num)))
        results = []
        for i, async_result in tqdm.tqdm(list(enumerate(async_results))):
            if async_result.get():
                results.append(i * block_size)
        return results


def init_img_file(disk_path, block_size, blocks_num):
    target_len = block_size * (blocks_num - 1) + 1
    appendage_len = target_len
    if os.path.exists(disk_path):
        appendage_len = target_len - os.path.getsize(disk_path)
        if appendage_len <= 0:
            return
    with open(disk_path, 'ab') as fo:
        for _ in range(appendage_len):
            fo.write('\x00')


def download_block(s3_name, disk_path, block_pos, block_size):
    block_id = int(block_pos / block_size)
    response = CLIENT.get_object(
        Bucket=BUCKET,
        Key=f'{PREFIX}/{s3_name}/block_{block_id}.bin.gz'
    )
    # TODO: switch to zlib for speed
    block_contents = gzip.decompress(response['Body'].read())
    with open(disk_path, 'ab') as fo:
        fo.seek(block_pos)
        fo.write(block_contents)
    assert get_block_hash(disk_path, block_pos) == response['Metadata']['uncompressedsha1']


def async_download_blocks(s3_name, disk_path, blocks_to_download):
    metadata = download_metadata(s3_name)
    block_size = metadata['block_size']
    blocks_num = metadata['blocks_num']
    init_img_file(disk_path, block_size, blocks_num)
    with multiprocessing.Pool() as pool:
        async_results = []
        for block_pos in blocks_to_download:
            async_results.append(pool.apply_async(download_block, (s3_name, disk_path, block_pos, block_size)))
        results = []
        for async_result in tqdm.tqdm(async_results):
            async_result.wait()


def process_blocks(s3_name, disk_path):
    blocks_to_download = get_blocks_to_download(s3_name, disk_path)
    print(f'{len(blocks_to_download)} blocks need downloading. Starting now...')
    async_download_blocks(s3_name, disk_path, blocks_to_download)
    print('Download complete')


def main():
    parser = argparse.ArgumentParser(
        prog='./s3blocks2img.py',
        description='Downloads compressed blocks from S3 to local .img file, minding the changed block'
    )
    parser.add_argument('s3_name', help='Disk image name in S3')
    parser.add_argument('disk_path', help='Local path to the .img file')
    args = parser.parse_args()
    process_blocks(args.s3_name, args.disk_path)

if __name__ == '__main__':
    main()
