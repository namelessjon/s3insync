import os

import boto3
import moto
import pytest

import s3insync.repositories as r


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with moto.mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture()
def aws_bucket(s3):
    s3.create_bucket(Bucket="example")
    s3.put_object(Bucket="example", Key="path/a", Body=b'a')
    s3.put_object(Bucket="example", Key="path/b", Body=b'b')
    s3.put_object(Bucket="example", Key="path/c/d", Body=b'e')

    return s3


@pytest.fixture()
def aws_ab_repo(aws_bucket):
    aws = r.S3Repo("aws", "s3://example/path", aws_bucket)

    return aws


def test_aws_repo_takes_name_s3uri():
    aws = r.S3Repo("aws", "s3://example/path")

    assert aws.name == "aws"
    assert aws.uri == "s3://example/path"


def test_aws_repo_takes_name_s3uri_client(s3):
    aws = r.S3Repo("aws", "s3://example/path", s3)

    assert aws.name == "aws"
    assert aws.uri == "s3://example/path"
    assert aws.client == s3


def test_aws_repo_entries_is_files_from_the_path(aws_ab_repo):
    entries = set(aws_ab_repo)

    assert entries == {r.Entry("a", "0cc175b9c0f1b6a831c399e269772661"),
                       r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                       r.Entry("c/d", "e1671797c52e15f763380b45e841ec32"),
                       }


def test_aws_repo_entries_is_files_from_the_path_even_if_there_are_lots(aws_ab_repo):
    aws_ab_repo.maxkeys = 1

    entries = set(aws_ab_repo)

    assert entries == {r.Entry("a", "0cc175b9c0f1b6a831c399e269772661"),
                       r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                       r.Entry("c/d", "e1671797c52e15f763380b45e841ec32"),
                       }


def test_aws_repo_can_retrieve_contents_by_path(aws_ab_repo):
    contents = aws_ab_repo.contents('a')

    assert contents.read() == b'a'
    assert contents.content_id == "0cc175b9c0f1b6a831c399e269772661"


def test_aws_repo_raises_key_error_if_file_doesnt_exist(aws_ab_repo):
    with pytest.raises(KeyError):
        aws_ab_repo.contents('c')


def test_aws_repo_entries_also_works_with_trailing_slash(aws_bucket):
    repo = r.S3Repo("aws", "s3://example/path/", aws_bucket)
    entries = set(repo)

    assert entries == {r.Entry("a", "0cc175b9c0f1b6a831c399e269772661"),
                       r.Entry("b", "92eb5ffee6ae2fec3ad71c777531578f"),
                       r.Entry("c/d", "e1671797c52e15f763380b45e841ec32"),
                       }


def test_aws_repo_entries_works_with_empty_bucket(aws_bucket):
    repo = r.S3Repo("aws", "s3://example/nonexistantpath/", aws_bucket)
    entries = set(repo)

    assert not entries
