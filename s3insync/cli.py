import logging
import os
import os.path
import time

import prometheus_client as pc

import s3insync
import s3insync.sync_decider as sd
import s3insync.repositories as r

logger = logging.getLogger()


def run_pull(args):
    s3uri = args.s3uri
    localpath = args.localpath
    excludes = args.exclude
    interval = args.interval

    i = pc.Info('s3insync_version', 'Version and config information for the client')
    i.info({'version': s3insync.__version__, 'aws_repo': s3uri, 'localpath': localpath,})
    start_sync = pc.Gauge('s3insync_last_sync_time', 'Time the last sync was started')
    start_time = pc.Gauge('s3insync_start_time', 'Time the sync process was started')
    op_count = pc.Counter('s3insync_operations', 'Count of operations', labelnames=('type',))
    failed_op_count = pc.Counter('s3insync_failed_operations', 'Count of failed operations', labelnames=('type',))
    files_in_s3 = pc.Gauge('s3insync_files_in_s3', 'Number of files in S3',)

    pc.start_http_server(8087)
    src = r.AwsRepo('aws', s3uri)
    dest = r.LocalFSRepo('fs', localpath, os.path.join(os.getenv('HOME'), ".s3insync"))

    sync = sd.SyncDecider(excludes)

    while True:
        logger.info("Starting sync")
        start = time.monotonic()
        start_sync.set_to_current_time()

        success, failures = sync.execute_sync(src, dest)
        files_in_s3.set(success.pop('total'))
        for t, count in success.items():
            op_count.labels(t).inc(count)
        for t, count in failures.items():
            failed_op_count.labels(t).inc(count)

        stop = time.monotonic()
        duration = stop - start
        logger.info("Stopping sync")
        time.sleep(max(30, interval - duration))


def main():
    import argparse

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_pull = subparsers.add_parser('pull', help='pull regularly from s3 to the local filesystem')

    parser_pull.add_argument('s3uri', help="URI of the S3 repo")
    parser_pull.add_argument('localpath', help="Path to sync to")

    parser_pull.add_argument('-e', '--exclude', action='append', help='Files to exclude from syncing or deleting', default=[])

    parser_pull.add_argument('-i', '--interval', type=int, default=300, help='Interval between syncing')

    parser_pull.set_defaults(func=run_pull)


    args = parser.parse_args()
    args.func(args)

