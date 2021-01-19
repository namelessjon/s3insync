import logging

import s3insync.cmd.pull as pull


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.set_defaults(func=lambda x: parser.print_help())

    parser.add_argument('--debug', action='store_true', default=False, help="Run with debug logging")
    parser.add_argument('--log-level', default="INFO", help="set log level", choices=('INFO', 'WARN', 'ERROR', ))

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_pull = subparsers.add_parser('pull', help='pull regularly from s3 to the local filesystem')

    parser_pull.add_argument('s3uri', help="URI of the S3 repo")
    parser_pull.add_argument('localpath', help="Path to sync to")

    parser_pull.add_argument('-e', '--exclude', action='append', help='Files to exclude from syncing or deleting', default=[])

    parser_pull.add_argument('-i', '--interval', type=int, default=300, help='Interval between syncing')

    parser_pull.set_defaults(func=pull.run)

    args = parser.parse_args()

    if args.debug:
        level = 'DEBUG'
    else:
        level = args.log_level
    setup_logging(level)

    args.func(args)


def setup_logging(level):
    logging.basicConfig(level=level)

    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
