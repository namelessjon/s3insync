import os
import io

import pytest

import s3insync.repositories as r


@pytest.fixture()
def fake_local_filesystem(fs):
    os.makedirs('repository')
    os.makedirs('repository/c')
    os.makedirs('staging')

    with open("repository/a", "w") as f:
        f.write("a")

    with open("repository/b", "w") as f:
        f.write("b")

    with open("repository/c/d", "w") as f:
        f.write("d")

    return fs


@pytest.fixture()
def local_repo(fake_local_filesystem):
    return r.LocalFSRepo("local", "repository", "staging")


def test_localfs_repo_takes_name_root(fake_local_filesystem):
    local = r.LocalFSRepo("local", "repository", "staging")

    assert local.name == "local"
    assert local.root == "repository"
    assert local.staging == "staging"


def test_localfs_repo_entries_is_files_from_the_path(local_repo):
    entries = list(local_repo)

    assert entries == [r.Entry("a", "0cc175b9c0f1b6a831c399e269772661"),
                       r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                       r.Entry(path='c/d', content_id='8277e0910d750195b448797616e091ad'),
                       ]


def test_localfs_repo_allows_writing_to_path(local_repo):
    contents = r.Contents("e", "e", io.BytesIO(b'e'))
    local_repo.write(contents)

    assert os.path.exists('repository/e')
