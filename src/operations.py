import dataclasses as dc


@dc.dataclass()
class Copy:
    path: str
    from_repo: object
    to_repo: object


@dc.dataclass()
class Delete:
    path: str
    from_repo: object
    to_repo: object