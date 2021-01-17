import s3insync.sync_decider as sd
import s3insync.repositories as r
import s3insync.operations as o


def test_a_new_will_be_synced():
    syncd = sd.SyncDecider()
    from_repo = r.TestRepo("from", ["a"])
    to_repo = r.TestRepo("to")

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Copy("a", from_repo, to_repo)]


def test_an_identical_file_will_not_be_synced():
    syncd = sd.SyncDecider()
    from_repo = r.TestRepo("from", ["a"])
    to_repo = r.TestRepo("to", ["a"])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Nop("a", from_repo, to_repo)]


def test_a_missing_file_will_be_deleted():
    syncd = sd.SyncDecider()
    from_repo = r.TestRepo("from", ["a"])
    to_repo = r.TestRepo("to", ["a", "b"])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Nop("a", from_repo, to_repo), o.Delete("b", from_repo, to_repo)]


def test_an_excluded_file_is_not_synced():
    syncd = sd.SyncDecider(excludes=["b"])
    from_repo = r.TestRepo("from", ["a", "b"])
    to_repo = r.TestRepo("to", ["a"])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Nop("a", from_repo, to_repo), o.Excluded("b", from_repo, to_repo)]


def test_an_excluded_file_is_not_deleted():
    syncd = sd.SyncDecider(excludes=["b"])
    from_repo = r.TestRepo("from", ["a"])
    to_repo = r.TestRepo("to", ["a", "b"])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Nop("a", from_repo, to_repo)]


def test_an_excludes_can_be_globs():
    syncd = sd.SyncDecider(excludes=["b*", "c*"])
    from_repo = r.TestRepo("from", ["a"])
    to_repo = r.TestRepo("to", ["a", "be"])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Nop("a", from_repo, to_repo)]


def test_a_present_file_whose_content_has_changed_will_be_synced():
    syncd = sd.SyncDecider()
    from_repo = r.TestRepo("from", [r.Entry("a", "2")])
    to_repo = r.TestRepo("to", [r.Entry("a", "1")])

    ops = list(syncd.sync(from_repo, to_repo))

    assert ops == [o.Copy("a", from_repo, to_repo)]
