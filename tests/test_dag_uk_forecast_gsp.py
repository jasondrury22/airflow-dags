"""Unit tests for business logic functions in uk/forecast-gsp-dag.py."""

import datetime as dt
import importlib.util
import os
import unittest
from unittest.mock import patch

_DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/airflow_dags/dags"))
_spec = importlib.util.spec_from_file_location(
    "uk_forecast_gsp",
    os.path.join(_DAGS_DIR, "uk/forecast-gsp-dag.py"),
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


def _make_last_run(hours_ago: float) -> dt.datetime:
    """Return a UTC datetime `hours_ago` hours before now."""
    return dt.datetime.now(tz=dt.UTC) - dt.timedelta(hours=hours_ago)


class TestGetForecastLastRunFromApi(unittest.TestCase):
    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_parses_datetime_from_response(self, _mock_requests) -> None:  # noqa: ANN001
        with patch.object(_mod, "requests") as mock_req:
            mock_response = unittest.mock.MagicMock()
            mock_response.json.return_value = "2025-06-15T10:30:00.000000Z"
            mock_req.get.return_value = mock_response

            result = _mod.get_forecast_last_run_from_api("pvnet_v2")

        self.assertEqual(result.year, 2025)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.hour, 10)
        self.assertEqual(result.minute, 30)
        self.assertIsNotNone(result.tzinfo)

    def test_url_contains_model_name(self) -> None:
        with patch.object(_mod, "requests") as mock_req:
            mock_response = unittest.mock.MagicMock()
            mock_response.json.return_value = "2025-01-01T00:00:00.000000Z"
            mock_req.get.return_value = mock_response

            _mod.get_forecast_last_run_from_api("pvnet_day_ahead")

        call_url = mock_req.get.call_args[0][0]
        self.assertIn("pvnet_day_ahead", call_url)


class TestCheckForecastFailureIsSubcritical(unittest.TestCase):
    """Three branches:
    1. All three models within 2 hours → subcritical message
    2. PVNet late but PVNet DA on time → subcritical "PVNet failed" message
    3. Both PVNet and PVNet DA late → raises Exception (escalate to critical)
    """

    def _patch_last_runs(self, pvnet_hours_ago: float, ecmwf_hours_ago: float, da_hours_ago: float):
        runs = {
            "pvnet_v2": _make_last_run(pvnet_hours_ago),
            "pvnet_ecmwf": _make_last_run(ecmwf_hours_ago),
            "pvnet_day_ahead": _make_last_run(da_hours_ago),
        }
        return patch.object(_mod, "get_forecast_last_run_from_api", side_effect=lambda m: runs[m])

    def test_all_models_recent_returns_subcritical_all_ok_message(self) -> None:
        with self._patch_last_runs(pvnet_hours_ago=1, ecmwf_hours_ago=1, da_hours_ago=1):
            result = _mod.check_forecast_failure_is_subcritical()
        self.assertIn("PVNet", result)
        self.assertIn("within the last", result)

    def test_pvnet_late_but_da_recent_returns_pvnet_failed_message(self) -> None:
        with self._patch_last_runs(pvnet_hours_ago=3, ecmwf_hours_ago=3, da_hours_ago=1):
            result = _mod.check_forecast_failure_is_subcritical()
        self.assertIn("PVNet has failed", result)
        self.assertIn("PVNet DA model has run", result)

    def test_both_pvnet_and_da_late_raises_exception(self) -> None:
        with self._patch_last_runs(pvnet_hours_ago=3, ecmwf_hours_ago=3, da_hours_ago=3):
            with self.assertRaises(Exception) as ctx:
                _mod.check_forecast_failure_is_subcritical()
        self.assertIn("critical", str(ctx.exception).lower())

    def test_models_just_within_threshold_returns_subcritical_all_ok_message(self) -> None:
        # Use 1 hour to stay safely within the 2-hour threshold
        with self._patch_last_runs(pvnet_hours_ago=1, ecmwf_hours_ago=1.5, da_hours_ago=0.5):
            result = _mod.check_forecast_failure_is_subcritical()
        self.assertIn("within the last", result)


class TestCheckForecastFailureIsCritical(unittest.TestCase):
    """Two branches:
    1. PVNet and PVNet DA both > 2 hours → includes "has failed" in message
    2. Otherwise → includes last run times in message
    """

    def _patch_last_runs(self, pvnet_hours_ago: float, ecmwf_hours_ago: float, da_hours_ago: float):
        runs = {
            "pvnet_v2": _make_last_run(pvnet_hours_ago),
            "pvnet_ecmwf": _make_last_run(ecmwf_hours_ago),
            "pvnet_day_ahead": _make_last_run(da_hours_ago),
        }
        return patch.object(_mod, "get_forecast_last_run_from_api", side_effect=lambda m: runs[m])

    def test_both_late_message_mentions_failed(self) -> None:
        with self._patch_last_runs(pvnet_hours_ago=3, ecmwf_hours_ago=3, da_hours_ago=3):
            result = _mod.check_forecast_failure_is_critical()
        self.assertIn("failed", result.lower())

    def test_message_always_includes_last_run_times(self) -> None:
        with self._patch_last_runs(pvnet_hours_ago=3, ecmwf_hours_ago=1, da_hours_ago=1):
            result = _mod.check_forecast_failure_is_critical()
        # Should include formatted datetimes
        self.assertIn("Last success run of PVNet", result)

    def test_returns_string_in_all_branches(self) -> None:
        for pvnet_ago, da_ago in [(3, 3), (3, 1), (1, 1)]:
            with self.subTest(pvnet_ago=pvnet_ago, da_ago=da_ago):
                with self._patch_last_runs(pvnet_hours_ago=pvnet_ago, ecmwf_hours_ago=1, da_hours_ago=da_ago):
                    result = _mod.check_forecast_failure_is_critical()
                self.assertIsInstance(result, str)
                self.assertGreater(len(result), 0)


if __name__ == "__main__":
    unittest.main()
