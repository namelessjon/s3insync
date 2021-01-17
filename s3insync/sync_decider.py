import collections
import fnmatch
import logging
import re
import typing as t

import s3insync.operations as op


log = logging.getLogger(__name__)


class SyncDecider:
    def __init__(self, excludes=None):
        if excludes is not None:
            self.excludes = re.compile('|'.join(map(fnmatch.translate, excludes)))
        else:
            self.excludes = None

    def sync(self, from_repo, to_repo):
        seen_paths = {e.path: False for e in to_repo}
        for entry in from_repo:
            path = entry.path

            if self.entry_excluded(path):
                yield op.Excluded(path, from_repo, to_repo)
                continue

            if path not in seen_paths:
                yield op.Copy(path, from_repo, to_repo)
            elif path in seen_paths and entry != to_repo.get(path):
                yield op.Copy(path, from_repo, to_repo)
            else:
                yield op.Nop(path, from_repo, to_repo)

            seen_paths[entry.path] = True

        for entry, seen in seen_paths.items():
            if seen is False and not self.entry_excluded(entry):
                yield op.Delete(entry, from_repo, to_repo)

    def execute_sync(self, from_repo, to_repo) -> t.Dict[str, int]:
        counts = collections.Counter()
        for operation in self.sync(from_repo, to_repo):
            success = operation.execute()
            if not success:
                counts[f"{operation.name}.failure"] += 1
                log.error(f"Failed to execute {operation}")
            counts[operation.name] += 1

        return {**counts}

    def entry_excluded(self, entry: str) -> bool:
        if self.excludes is None:
            return False
        return True if self.excludes.match(entry) else False
