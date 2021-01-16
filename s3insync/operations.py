import dataclasses as dc


@dc.dataclass()
class Copy:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        contents = self.from_repo.contents(self.path)

        self.to_repo.write(self.path, contents)


@dc.dataclass()
class Delete:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        self.to_repo.delete(self.path)
