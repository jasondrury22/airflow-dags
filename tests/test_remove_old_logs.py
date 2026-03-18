"""Unit tests for plugins/scripts/remove_old_logs.py."""

import os
import tempfile
import time
import unittest


def _write_file(path: str, content: str = "log data") -> None:
    with open(path, "w") as f:
        f.write(content)


def _set_old_mtime(path: str, days: int = 8) -> None:
    """Set a file's mtime to `days` days in the past."""
    old_time = time.time() - (days * 86400)
    os.utime(path, (old_time, old_time))


SEVEN_DAYS_MS = 7 * 86400 * 1000


class TestDeleteOldLogsAndEmptyDirs(unittest.TestCase):
    def test_deletes_file_older_than_threshold(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            old_file = os.path.join(tmpdir, "old.log")
            _write_file(old_file)
            _set_old_mtime(old_file, days=8)

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertFalse(os.path.exists(old_file))

    def test_keeps_file_newer_than_threshold(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            new_file = os.path.join(tmpdir, "new.log")
            _write_file(new_file)
            # mtime defaults to now — well within threshold

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertTrue(os.path.exists(new_file))

    def test_removes_empty_subdirectory(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            # Anchor file so tmpdir itself is not empty and won't be rmdir'd
            _write_file(os.path.join(tmpdir, "anchor.log"))
            subdir = os.path.join(tmpdir, "empty_subdir")
            os.makedirs(subdir)

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertFalse(os.path.exists(subdir))

    def test_does_not_remove_nonempty_subdirectory(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "active_subdir")
            os.makedirs(subdir)
            _write_file(os.path.join(subdir, "recent.log"))

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertTrue(os.path.exists(subdir))

    def test_skips_dag_processor_manager_directory(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            skip_dir = os.path.join(tmpdir, "dag_processor_manager")
            os.makedirs(skip_dir)
            old_file = os.path.join(skip_dir, "old.log")
            _write_file(old_file)
            _set_old_mtime(old_file, days=10)

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertTrue(os.path.exists(old_file))

    def test_deletes_old_files_in_nested_subdir(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            # Anchor file at root so tmpdir is not deleted
            _write_file(os.path.join(tmpdir, "anchor.log"))
            subdir = os.path.join(tmpdir, "dag_id", "task_id", "run_id")
            os.makedirs(subdir)
            old_file = os.path.join(subdir, "attempt.log")
            _write_file(old_file)
            _set_old_mtime(old_file, days=9)

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertFalse(os.path.exists(old_file))

    def test_keeps_recent_and_deletes_old_in_same_directory(self) -> None:
        from airflow_dags.plugins.scripts.remove_old_logs import delete_old_logs_and_empty_dirs

        with tempfile.TemporaryDirectory() as tmpdir:
            old_file = os.path.join(tmpdir, "old.log")
            new_file = os.path.join(tmpdir, "new.log")
            _write_file(old_file)
            _write_file(new_file)
            _set_old_mtime(old_file, days=8)

            delete_old_logs_and_empty_dirs(tmpdir, age_threshold_ms=SEVEN_DAYS_MS)

            self.assertFalse(os.path.exists(old_file))
            self.assertTrue(os.path.exists(new_file))


if __name__ == "__main__":
    unittest.main()
