import dataclasses as dc
import hashlib
import os
import typing as t
import urllib.parse as up
import shutil

import boto3


@dc.dataclass(frozen=True)
class Entry:
    path: str
    content_id: str


@dc.dataclass()
class Contents:
    path: str
    content_id: str
    body: object

    def __getattr__(self, name: str):
        if hasattr(self.body, name):
            return getattr(self.body, name)


class AwsRepo:
    def __init__(self, name: str, uri: str, client=None, maxkeys=1000):
        self.name = name
        self.uri = uri
        parsed = up.urlparse(self.uri)
        self.bucket = parsed.netloc
        self.prefix = parsed.path[1:]
        if client is None:
            self.client = boto3.client('s3')
        else:
            self.client = client
        self.maxkeys = maxkeys

    def __iter__(self):
        token = None
        while True:
            args = {
                "Bucket": self.bucket,
                "Prefix": self.prefix,
                "MaxKeys": self.maxkeys,
            }
            if token:
                args['ContinuationToken'] = token

            response = self.client.list_objects_v2(**args)

            for entry in response['Contents']:
                key = entry['Key'][len(self.prefix) + 1:]
                yield Entry(key, entry['ETag'].strip('"'))
            token = response.get('NextContinuationToken')
            if token is None:
                break

    def contents(self, path: str):
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=f'{self.prefix}/{path}')

            return Contents(path, obj['ETag'].strip('"'), obj['Body'])
        except self.client.exceptions.NoSuchKey:
            raise KeyError(f"Object '{path}' not found in {self.name}")


class LocalFSRepo:
    def __init__(self, name: str, root: str, staging: str):
        self.name = name
        self.root = root
        self.staging = staging

    def __iter__(self):
        for dirpath, dirnames, filenames in os.walk(self.root):

            prefix = dirpath[len(self.root) + 1:]
            for fn in filenames:
                path = os.path.join(prefix, fn)
                md5 = self.md5_file(path)
                yield Entry(path, md5)

    def md5_file(self, path: str):
        full_path = os.path.join(self.root, path)
        hsh = hashlib.md5()
        with open(full_path, "rb") as f:
            while True:
                dat = f.read(4096)
                if dat == b'':
                    break
                hsh.update(dat)

        return hsh.hexdigest()

    def write(self, contents: Contents):
        temp_name = hashlib.md5(contents.path.encode()).hexdigest()
        temp_path = os.path.join(self.staging, temp_name)

        with os.fdopen(os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY), "wb") as f:
            shutil.copyfileobj(contents.body, f)

        full_path = os.path.join(self.root, contents.path)
        shutil.move(temp_path, full_path)


class TestRepo:
    def __init__(self, name: str, entries=None):
        self.name = name
        self.entries = {}
        if entries is not None:
            for entry in entries:
                if isinstance(entry, str):
                    self.add_entry(Entry(entry, entry))
                else:
                    self.add_entry(entry)

    def add_entry(self, entry: Entry):
        self.entries[entry.path] = entry

    def __iter__(self) -> t.Iterator[Entry]:
        for entry in self.entries.values():
            yield entry

    def get(self, path) -> Entry:
        return self.entries.get(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"
