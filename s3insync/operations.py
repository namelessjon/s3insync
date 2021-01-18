import dataclasses as dc


@dc.dataclass()
class Copy:
    path: str
    from_repo: object
    to_repo: object

    name = "copy"

    def execute(self):
        contents = self.from_repo.contents(self.path)

        return self.to_repo.write(self.path, contents)


@dc.dataclass()
class Delete:
    path: str
    from_repo: object
    to_repo: object

    name = "delete"

    def execute(self):
        return self.to_repo.delete(self.path)


@dc.dataclass()
class Nop:
    path: str
    from_repo: object
    to_repo: object

    name = "nop"

    def execute(self):
        return True


@dc.dataclass()
class Excluded:
    path: str
    from_repo: object
    to_repo: object

    name = "excluded"

    def execute(self):
        return True
