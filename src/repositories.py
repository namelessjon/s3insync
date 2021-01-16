import dataclasses as dc
import typing as t

@dc.dataclass(frozen=True)
class Entry:
    path: str
    content_id: str


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
            


