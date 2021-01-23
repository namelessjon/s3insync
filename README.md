S3InSync
========

![Python package](https://github.com/namelessjon/s3insync/workflows/Python%20package/badge.svg)

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

---

Enable debug logs by passing the `--debug` flag `s3insync --debug pull ...`


Installation
------------

Simply download the prebuilt single binary from [release page](https://github.com/namelessjon/s3insync/releases) or use `pip` command:

```bash
python3 -m pip install s3insync
```

Development
------------

Run tests

```bash
pytest
```

Run from source

```bash
python -m s3insync pull --exclude '*/__pycache__/*' s3://bucket/prefix ./localdir
```