import dataclasses as dc


@dc.dataclass()
class Copy:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        contents = self.from_repo.contents(self.path)

        return self.to_repo.write(self.path, contents)

    def name(self) -> str:
        return "copy"


@dc.dataclass()
class Delete:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        return self.to_repo.delete(self.path)

    def name(self) -> str:
        return "delete"


@dc.dataclass()
class Nop:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        return True

    def name(self) -> str:
        return "nop"


@dc.dataclass()
class Excluded:
    path: str
    from_repo: object
    to_repo: object

    def execute(self):
        return True

    def name(self) -> str:
        return "excluded"
