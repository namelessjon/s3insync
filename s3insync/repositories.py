import dataclasses as dc
import hashlib
import logging
import os
import tempfile
import typing as t
import urllib.parse as up
import shutil

import boto3


log = logging.getLogger(__name__)


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

    def __enter__(self):
        return self

    def __exit__(self, _e, _v, _t):
        self.close()


class S3Repo:
    def __init__(self, name: str, uri: str, client=None, maxkeys=1000):
        self.name = name
        self.uri = uri
        parsed = up.urlparse(self.uri)
        self.bucket = parsed.netloc
        self.prefix = parsed.path[1:]

        if not self.prefix.endswith('/'):
            self.prefix = self.prefix + "/"
        if client is None:
            self.client = boto3.client('s3')
        else:
            self.client = client
        self.maxkeys = maxkeys

    def __iter__(self) -> t.Iterator[Entry]:
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

            for entry in response.get('Contents', []):
                key = entry['Key'][len(self.prefix):]
                yield Entry(key, entry['ETag'].strip('"'))
            token = response.get('NextContinuationToken')
            if token is None:
                break

    def contents(self, path: str):
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=f'{self.prefix}{path}')

            return Contents(path, obj['ETag'].strip('"'), obj['Body'])
        except self.client.exceptions.NoSuchKey:
            raise KeyError(f"Object '{path}' not found in {self.name}")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r}, uri={self.uri!r})"


class LocalFSRepo:
    def __init__(self, name: str, root: str, staging: str):
        self.name = name
        self.root = root
        self.staging = staging
        self._entries = None

    @property
    def entries(self):
        if self._entries is None:
            self._entries = {e.path: e for e in self.walk_repo()}
        return self._entries

    def walk_repo(self):
        for dirpath, dirnames, filenames in os.walk(self.root):

            prefix = dirpath[len(self.root) + 1:]
            for fn in filenames:
                path = os.path.join(prefix, fn)
                md5 = self.md5_file(path)
                yield Entry(path, md5)

    def contents(self, path):
        if path in self.entries:
            entry = self.entries[path]
            return Contents(entry.path, entry.content_id, open(self.fullpath(path), "rb"))
        else:
            raise KeyError(f"Object '{path}' not found in {self.name}")

    def fullpath(self, path: str) -> str:
        return os.path.join(self.root, path)

    def __iter__(self) -> t.Iterator[Entry]:
        yield from self.entries.values()

    def md5_file(self, path: str):
        full_path = self.fullpath(path)
        hsh = hashlib.md5()
        with open(full_path, "rb") as f:
            dat = f.read(4096)
            while dat:
                hsh.update(dat)
                dat = f.read(4096)

        return hsh.hexdigest()

    def write(self, contents: Contents):
        try:
            temp_path = None
            with tempfile.NamedTemporaryFile(dir=self.staging, delete=False) as f:
                temp_path = f.name
                shutil.copyfileobj(contents.body, f)

            full_path = self.fullpath(contents.path)

            dirname = os.path.dirname(full_path)
            os.makedirs(dirname, exist_ok=True)
            shutil.move(temp_path, full_path)
            self.entries[contents.path] = Entry(contents.path, contents.content_id)
            return True
        except OSError:
            log.exception("Problem writing %r to %r", contents.path, self)
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            return False

    def delete(self, path: str) -> bool:
        try:
            os.remove(self.fullpath(path))

            if path in self.entries:
                del self.entries[path]

            return True
        except FileNotFoundError:
            return True
        except OSError:
            log.exception("Problem deleting %r from %r", path, self)
            return False

    def ensure_directories(self):
        os.makedirs(self.root, exist_ok=True)
        os.makedirs(self.staging, exist_ok=True)

    def get(self, path):
        return self.entries.get(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r}, root={self.root!r})"


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
