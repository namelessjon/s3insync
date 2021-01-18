import os
import io

import pytest

import s3insync.repositories as r


@pytest.fixture()
def fake_local_filesystem(fs):
    os.makedirs('repository')
    os.makedirs('staging')

    with open("repository/a", "w") as f:
        f.write("a")

    with open("repository/b", "w") as f:
        f.write("b")

    fs.create_file("repository/c/d", contents='d')
    os.chmod('repository/c/d', 0o444)
    os.chmod('repository/c', 0o555)

    fs.set_disk_usage(100)

    return fs


@pytest.fixture()
def fake_local_filesystem_empty(fs):
    fs.set_disk_usage(100)

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


def test_localfs_repo_allows_getting_contents_of_an_entry(local_repo):
    contents = local_repo.contents('a')

    assert contents.path == 'a'
    assert contents.content_id == "0cc175b9c0f1b6a831c399e269772661"


def test_localfs_repo_raises_a_key_error_if_the_content_doesnt_exist(local_repo):
    with pytest.raises(KeyError):
        local_repo.contents('c')


def test_localfs_repo_allows_getting_an_entry(local_repo):
    contents = local_repo.get('a')

    assert contents.path == 'a'
    assert contents.content_id == "0cc175b9c0f1b6a831c399e269772661"


def test_localfs_repo_get_returns_none_for_missing_key(local_repo):
    assert local_repo.get('c') is None


def test_localfs_repo_allows_writing_to_path(local_repo):
    contents = r.Contents("e", "e", io.BytesIO(b'e'))
    local_repo.write(contents)

    assert os.path.exists('repository/e')
    assert open('repository/e').read() == 'e'

    entries = list(local_repo)

    assert set(entries) == {r.Entry("a", "0cc175b9c0f1b6a831c399e269772661"),
                            r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                            r.Entry(path='c/d', content_id='8277e0910d750195b448797616e091ad'),
                            r.Entry("e", "e"),
                            }


def test_localfs_repo_allows_writing_to_path_which_is_nested(local_repo):
    contents = r.Contents("f/e", "e", io.BytesIO(b'e'))
    local_repo.write(contents)

    assert os.path.exists('repository/f/e')


def test_localfs_repo_wont_partially_write_a_file(local_repo):
    contents = r.Contents("a", "a", io.BytesIO(b'e' * 200))
    local_repo.write(contents)

    assert open('repository/a').read() == 'a'


def test_localfs_repo_allows_deleting_a_file(local_repo):
    assert local_repo.get('a') is not None
    assert local_repo.delete('a')
    assert not os.path.exists('repository/a')
    assert set(local_repo) == {r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                               r.Entry(path='c/d', content_id='8277e0910d750195b448797616e091ad'), }

    assert local_repo.get('a') is None


def test_localfs_repo_successfully_deletes_a_file_which_doesnt_exist(local_repo):
    assert local_repo.delete('f')


def test_localfs_repo_returns_false_if_it_cant_delete_file(local_repo):
    assert not local_repo.delete('c/d')


def test_localfs_can_set_up_directories(fake_local_filesystem_empty):
    local = r.LocalFSRepo("local", "repository", "staging")

    local.ensure_directories()

    assert os.path.isdir('repository')
    assert os.path.isdir('staging')
