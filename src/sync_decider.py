import fnmatch
import re

import src.operations as op


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
                continue

            if path not in seen_paths:
                yield op.Copy(path, from_repo, to_repo)
            if path in seen_paths and entry != to_repo.get(path):
                yield op.Copy(path, from_repo, to_repo)

            seen_paths[entry.path] = True

        for entry, seen in seen_paths.items():
            if seen is False and not self.entry_excluded(entry):
                yield op.Delete(entry, from_repo, to_repo)

    def entry_excluded(self, entry: str) -> bool:
        if self.excludes is None:
            return False
        return True if self.excludes.match(entry) else False
