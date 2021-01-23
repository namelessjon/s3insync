import logging
import os
import os.path
import threading
import time
import typing as t
import signal

import prometheus_client as pc

import s3insync
import s3insync.repositories as r
import s3insync.sync_decider as sd

logger = logging.getLogger()


def run(args):
    s3uri = args.s3uri
    localpath = args.localpath
    excludes = args.exclude
    interval = args.interval

    i = pc.Info('s3insync_version', 'Version and config information for the client')
    i.info({'version': s3insync.__version__, 'aws_repo': s3uri, 'localpath': localpath, })
    start_time = pc.Gauge('s3insync_start_time', 'Time the sync process was started')
    start_time.set_to_current_time()

    last_sync = pc.Gauge('s3insync_last_sync_time', 'Time the last sync completed')
    op_count = pc.Counter('s3insync_operations', 'Count of operations', labelnames=('type',))
    failed_op_count = pc.Counter('s3insync_failed_operations', 'Count of failed operations', labelnames=('type',))
    files_in_s3 = pc.Gauge('s3insync_files_in_s3', 'Number of files in S3',)

    pc.start_http_server(8087)
    src = r.S3Repo('s3', s3uri)
    dest = r.LocalFSRepo('fs', localpath, os.path.join(os.getenv('HOME'), ".s3insync"))
    dest.ensure_directories()

    sync = sd.SyncDecider(excludes)

    set_exit = setup_signals()

    while not set_exit.is_set():
        logger.debug("Starting sync")
        start = time.monotonic()

        try:
            success, failures = sync.execute_sync(src, dest)
            files_in_s3.set(success.pop('total', 0))
            set_op_counts(success, op_count)
            set_op_counts(failures, failed_op_count)
            last_sync.set_to_current_time()
        except Exception:
            logger.exception("Failed to excute sync")

        duration = time.monotonic() - start
        logger.debug("Stopping sync after %g secs", duration)

        set_exit.wait(max(30, interval - duration))


def set_op_counts(items: t.Dict[str, int], metric: pc.Counter):
    for typ, count in items.items():
        metric.labels(typ).inc(count)


def setup_signals() -> threading.Event:
    set_exit = threading.Event()

    def quit(signo, _frame):
        logger.info("Received signal(%d), shutting down", signo)
        set_exit.set()

    for sig in ('SIGTERM', 'SIGHUP', 'SIGINT'):
        signal.signal(getattr(signal, sig), quit)

    return set_exit
