"""Unit tests for business logic functions in india/check-api.py."""

import datetime as dt
import importlib.util
import os
import unittest
from unittest.mock import patch

_DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/airflow_dags/dags"))
_spec = importlib.util.spec_from_file_location(
    "india_check_api",
    os.path.join(_DAGS_DIR, "india/check-api.py"),
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


def _recent_iso(minutes_ago: float = 10) -> str:
    """Return an ISO 8601 datetime string `minutes_ago` minutes in the past."""
    ts = dt.datetime.now(tz=dt.UTC) - dt.timedelta(minutes=minutes_ago)
    return ts.isoformat()


def _old_iso(hours_ago: float = 2) -> str:
    """Return an ISO 8601 datetime string `hours_ago` hours in the past."""
    ts = dt.datetime.now(tz=dt.UTC) - dt.timedelta(hours=hours_ago)
    return ts.isoformat()


class TestCheckSites(unittest.TestCase):
    def test_passes_with_valid_sites(self) -> None:
        data = [{"site_uuid": "abc-123"}, {"site_uuid": "def-456"}]
        with patch.object(_mod, "call_api", return_value=data):
            _mod.check_sites("token")

    def test_raises_when_no_sites_returned(self) -> None:
        with patch.object(_mod, "call_api", return_value=[]):
            with self.assertRaises(ValueError):
                _mod.check_sites("token")

    def test_raises_when_site_uuid_key_missing(self) -> None:
        data = [{"name": "site-without-uuid"}]
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_sites("token")


class TestCheckForecast(unittest.TestCase):
    def _make_forecast_values(self, count: int) -> list[dict]:
        return [{"Time": "2025-01-01T00:00:00", "PowerKW": 100.0} for _ in range(count)]

    def _patch_call_api(self, sites: list[dict], forecast_values: list[dict]):
        """Patch call_api to return sites on first call, forecast_values on subsequent calls."""
        responses = [sites] + [forecast_values] * len(sites)
        return patch.object(_mod, "call_api", side_effect=responses)

    def test_passes_with_sufficient_forecast_data(self) -> None:
        sites = [{"site_uuid": "abc"}]
        # Minimum: 2 * 24 * 2 + 30 * 2 = 96 + 60 = 156 values
        forecast_values = self._make_forecast_values(160)
        with self._patch_call_api(sites, forecast_values):
            _mod.check_forecast("token")

    def test_raises_when_forecast_too_short(self) -> None:
        sites = [{"site_uuid": "abc"}]
        with self._patch_call_api(sites, [{"Time": "t", "PowerKW": 1.0}]):
            with self.assertRaises(ValueError):
                _mod.check_forecast("token")

    def test_raises_when_time_key_missing(self) -> None:
        sites = [{"site_uuid": "abc"}]
        forecast_values = [{"PowerKW": 100.0} for _ in range(160)]
        with self._patch_call_api(sites, forecast_values):
            with self.assertRaises(ValueError):
                _mod.check_forecast("token")

    def test_raises_when_power_key_missing(self) -> None:
        sites = [{"site_uuid": "abc"}]
        forecast_values = [{"Time": "t"} for _ in range(160)]
        with self._patch_call_api(sites, forecast_values):
            with self.assertRaises(ValueError):
                _mod.check_forecast("token")


class TestCheckGeneration(unittest.TestCase):
    def _make_generation_values(self, times: list[str]) -> list[dict]:
        return [{"Time": t, "PowerKW": 50.0} for t in times]

    def _patch_call_api(self, sites: list[dict], generation_values: list[dict]):
        responses = [sites] + [generation_values] * len(sites)
        return patch.object(_mod, "call_api", side_effect=responses)

    def test_passes_with_recent_non_ruvnl_data(self) -> None:
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        values = self._make_generation_values([_recent_iso(minutes_ago=10)])
        with self._patch_call_api(sites, values):
            _mod.check_generation("token")

    def test_raises_when_data_older_than_one_hour_for_non_ruvnl(self) -> None:
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        values = self._make_generation_values([_old_iso(hours_ago=2)])
        with self._patch_call_api(sites, values):
            with self.assertRaises(ValueError):
                _mod.check_generation("token")

    def test_skips_datetime_check_for_ruvnl_sites(self) -> None:
        # RUVNL sites have stale data by design — the datetime check must be skipped
        sites = [{"site_uuid": "abc", "client_site_name": "ruvnl-primary"}]
        values = self._make_generation_values([_old_iso(hours_ago=5)])  # very old
        with self._patch_call_api(sites, values):
            _mod.check_generation("token")  # should not raise

    def test_raises_when_no_generation_data(self) -> None:
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        with self._patch_call_api(sites, []):
            with self.assertRaises(ValueError):
                _mod.check_generation("token")

    def test_raises_when_time_key_missing(self) -> None:
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        values = [{"PowerKW": 50.0}]
        with self._patch_call_api(sites, values):
            with self.assertRaises((ValueError, KeyError)):
                _mod.check_generation("token")

    def test_raises_when_power_key_missing(self) -> None:
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        values = [{"Time": _recent_iso()}]
        with self._patch_call_api(sites, values):
            with self.assertRaises(ValueError):
                _mod.check_generation("token")

    def test_uses_most_recent_datetime_among_multiple(self) -> None:
        """The freshness check uses the MAX time — old entries should not cause failure."""
        sites = [{"site_uuid": "abc", "client_site_name": "uk-site"}]
        values = self._make_generation_values([
            _old_iso(hours_ago=3),    # old entry — should be ignored
            _recent_iso(minutes_ago=5),  # fresh entry — determines pass/fail
        ])
        with self._patch_call_api(sites, values):
            _mod.check_generation("token")  # should not raise


if __name__ == "__main__":
    unittest.main()
