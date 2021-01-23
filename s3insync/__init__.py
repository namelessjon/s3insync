"""
Synchronise an S3 directory to the local filesystem, as a daemon

Daemon to continuously and incrementally synchronize a directory from remote
object store to a local directory.  Inspired by [objinsync](https://github.com/scribd/objinsync) but written in
python and presently less fully featured.


Usage
-----

```bash
s3insync pull --exclude '*/__pycache__/*' s3://bucket/prefix ./localdir
```

When running in daemon mode, a prometheus metrics endpoint is served at
`:8087/metrics`.
"""

__version__ = '0.2.1'
