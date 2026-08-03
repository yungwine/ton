# pyright: reportPrivateUsage=false

import json
import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import final, override

import pytest
from consensus_explorer.file_index import FileIndex, FileIndexCallback, FileScanResult
from consensus_explorer.models import GroupData, UnnamedGroupInfo
from tonapi.ton_api import (
    Consensus_stats_events,
    Consensus_stats_id,
    Consensus_stats_timestampedEvent,
)


def _group(group_hash: bytes) -> UnnamedGroupInfo:
    return UnnamedGroupInfo(valgroup_hash=group_hash, group_start_est=1.0)


def test_remove_file_deletes_groups_without_remaining_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"

    file_a = stats_dir / "a.log"
    file_b = stats_dir / "b.log"
    _ = file_a.write_text("first")
    _ = file_b.write_text("second")

    groups_by_file: dict[Path, set[GroupData]] = {
        file_a.resolve(): {_group(b"g-only-a"), _group(b"g-shared")},
        file_b.resolve(): {_group(b"g-shared")},
    }

    def fake_scan(path: Path) -> FileScanResult:
        return FileScanResult(groups=groups_by_file[path.resolve()])

    index = FileIndex(stats_dir, db_path)
    monkeypatch.setattr(index, "_scan_file", fake_scan)

    with index._connect() as conn:
        _ = index._index_file(file_a, conn, 0, 2)
        _ = index._index_file(file_b, conn, 1, 2)

    with index._connect() as conn:
        changed_hashes = index._remove_file(file_a, conn)

    assert changed_hashes == {b"g-only-a", b"g-shared"}
    assert {group.valgroup_hash for group in index.get_all_groups()} == {b"g-shared"}
    assert index.get_files_for_group(b"g-only-a") == []
    assert index.get_files_for_group(b"g-shared") == [file_b.resolve()]


def test_index_file_removes_deleted_file_if_it_disappears_before_reindex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"

    file_a = stats_dir / "a.log"
    _ = file_a.write_text("first")

    def fake_scan(_path: Path) -> FileScanResult:
        return FileScanResult(groups={_group(b"g-only-a")})

    index = FileIndex(stats_dir, db_path)
    monkeypatch.setattr(index, "_scan_file", fake_scan)

    with index._connect() as conn:
        _ = index._index_file(file_a, conn, 0, 1)

    file_a.unlink()

    with index._connect() as conn:
        changed_hashes = index._index_file(file_a, conn, 0, 1)

    assert changed_hashes == {b"g-only-a"}
    assert index.get_all_groups() == []
    assert index.get_files_for_group(b"g-only-a") == []


def test_index_file_skips_directory_and_removes_stale_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"

    file_a = stats_dir / "a.log"
    _ = file_a.write_text("first")

    def fake_scan(_path: Path) -> FileScanResult:
        return FileScanResult(groups={_group(b"g-only-a")})

    index = FileIndex(stats_dir, db_path)
    monkeypatch.setattr(index, "_scan_file", fake_scan)

    with index._connect() as conn:
        _ = index._index_file(file_a, conn, 0, 1)

    file_a.unlink()
    file_a.mkdir()

    with index._connect() as conn:
        changed_hashes = index._index_file(file_a, conn, 0, 1)

    assert changed_hashes == {b"g-only-a"}
    assert index.get_all_groups() == []
    assert index.get_files_for_group(b"g-only-a") == []


def test_initial_scan_skips_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"

    file_a = stats_dir / "a.log"
    _ = file_a.write_text("first")
    ignored_dir = stats_dir / "nested"
    ignored_dir.mkdir()

    groups_by_file: dict[Path, set[GroupData]] = {file_a.resolve(): {_group(b"g-only-a")}}

    def fake_scan(path: Path) -> FileScanResult:
        return FileScanResult(groups=groups_by_file[path.resolve()])

    index = FileIndex(stats_dir, db_path)
    monkeypatch.setattr(index, "_scan_file", fake_scan)

    with index._connect() as conn:
        index._initial_scan(conn)

    assert {group.valgroup_hash for group in index.get_all_groups()} == {b"g-only-a"}
    assert index.get_files_for_group(b"g-only-a") == [file_a.resolve()]


def test_scan_file_skips_model_errors(tmp_path: Path):
    db_path = tmp_path / "index.db"
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    log_file = tmp_path / "broken.log"
    valid_group_hash = b"g" * 32
    valid_line = json.dumps(
        Consensus_stats_events(
            id=valid_group_hash,
            events=[
                Consensus_stats_timestampedEvent(
                    ts=1.0,
                    event=Consensus_stats_id(
                        workchain=0,
                        shard=0,
                        cc_seqno=1,
                        idx=0,
                        total_validators=1,
                        weight=1,
                        total_weight=1,
                        slots_per_leader_window=1,
                    ),
                )
            ],
        ).to_dict(),
        separators=(",", ":"),
    )
    _ = log_file.write_text(
        f'{{"@type":"consensus.stats.events","id":1,"events":[]}}\n{valid_line}\n'
    )

    index = FileIndex(stats_dir, db_path)
    scan = index._scan_file(log_file)

    assert {group.valgroup_hash for group in scan.groups} == {valid_group_hash}


@final
class RecordingCallback(FileIndexCallback):
    def __init__(self):
        self.batches: list[set[bytes]] = []

    @override
    def on_files_changed(self, changed_groups: set[bytes]) -> None:
        self.batches.append(set(changed_groups))


def _writer_index(
    stats_dir: Path, db_path: Path, monkeypatch: pytest.MonkeyPatch, groups: dict[Path, set[bytes]]
) -> FileIndex:
    index = FileIndex(stats_dir, db_path)

    def fake_scan(path: Path) -> FileScanResult:
        return FileScanResult(groups={_group(h) for h in groups[path.resolve()]})

    monkeypatch.setattr(index, "_scan_file", fake_scan)
    return index


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_read_only_index_rejects_a_missing_database(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="read-only"):
        _ = FileIndex(tmp_path / "stats", tmp_path / "index.db", read_only=True)


def test_read_only_index_reads_without_writing_to_the_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"
    file_a = stats_dir / "a.log"
    _ = file_a.write_text("first")

    writer = _writer_index(stats_dir, db_path, monkeypatch, {file_a.resolve(): {b"g-a"}})
    with writer._connect() as conn:
        _ = writer._index_file(file_a, conn, 0, 1)

    reader = FileIndex(stats_dir, db_path, read_only=True)
    before = db_path.stat().st_mtime_ns

    assert {group.valgroup_hash for group in reader.get_all_groups()} == {b"g-a"}
    assert reader.get_files_for_group(b"g-a") == [file_a.resolve()]
    assert db_path.stat().st_mtime_ns == before
    # A read-only handle must refuse writes rather than corrupt someone's index.
    with reader._connect() as conn, pytest.raises(sqlite3.OperationalError, match="readonly"):
        _ = conn.execute("DELETE FROM groups")


def test_read_only_index_does_not_scan_the_stats_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"
    _ = (stats_dir / "a.log").write_text("first")

    # Seed an index that knows nothing about a.log, then confirm the reader
    # leaves it that way instead of indexing the file itself.
    _ = FileIndex(stats_dir, db_path)
    reader = FileIndex(stats_dir, db_path, read_only=True, poll_interval_seconds=0.01)

    def fail_scan(path: Path) -> FileScanResult:
        pytest.fail(f"read-only index scanned {path}")

    monkeypatch.setattr(reader, "_scan_file", fail_scan)

    with reader:
        time.sleep(0.1)
        assert reader.get_all_groups() == []


def test_read_only_index_invalidates_groups_the_owner_reindexes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"
    file_a = stats_dir / "a.log"
    file_b = stats_dir / "b.log"
    _ = file_a.write_text("first")
    _ = file_b.write_text("second")

    groups = {file_a.resolve(): {b"g-a"}, file_b.resolve(): {b"g-b"}}
    writer = _writer_index(stats_dir, db_path, monkeypatch, groups)
    with writer._connect() as conn:
        _ = writer._index_file(file_a, conn, 0, 2)

    callback = RecordingCallback()
    reader = FileIndex(stats_dir, db_path, read_only=True, poll_interval_seconds=0.01)
    reader.install_callback(callback)

    with reader:
        assert _wait_for(lambda: reader.get_all_groups() != [])
        assert callback.batches == []  # steady state stays quiet

        # The owner indexes a new file, then re-indexes an existing one.
        with writer._connect() as conn:
            _ = writer._index_file(file_b, conn, 1, 2)
        assert _wait_for(lambda: callback.batches != [])
        assert set().union(*callback.batches) == {b"g-b"}

        groups[file_a.resolve()] = {b"g-a", b"g-a2"}
        os.utime(file_a, (1.0, 1.0))
        with writer._connect() as conn:
            _ = writer._index_file(file_a, conn, 0, 2)
        assert _wait_for(lambda: b"g-a2" in set().union(*callback.batches))
        assert {b"g-a", b"g-a2"} <= set().union(*callback.batches)


def test_read_only_index_invalidates_groups_of_removed_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    db_path = tmp_path / "index.db"
    file_a = stats_dir / "a.log"
    _ = file_a.write_text("first")

    writer = _writer_index(stats_dir, db_path, monkeypatch, {file_a.resolve(): {b"g-a"}})
    with writer._connect() as conn:
        _ = writer._index_file(file_a, conn, 0, 1)

    callback = RecordingCallback()
    reader = FileIndex(stats_dir, db_path, read_only=True, poll_interval_seconds=0.01)
    reader.install_callback(callback)

    with reader:
        assert _wait_for(lambda: reader.get_all_groups() != [])
        with writer._connect() as conn:
            _ = writer._remove_file(file_a, conn)
        assert _wait_for(lambda: callback.batches != [])
        assert set().union(*callback.batches) == {b"g-a"}
