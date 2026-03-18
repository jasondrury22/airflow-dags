"""Unit tests for business logic functions in uk/check-api-national-gsp.py."""

import datetime as dt
import importlib.util
import os
import unittest
from unittest.mock import patch

# Load the hyphen-named module once at module level
_DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/airflow_dags/dags"))
_spec = importlib.util.spec_from_file_location(
    "uk_check_api_national_gsp",
    os.path.join(_DAGS_DIR, "uk/check-api-national-gsp.py"),
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

MIN_FORECAST_LEN = _mod.MIN_FORECAST_LENGTH_HOURS


def _make_forecast_value(
    target_time: str = "2025-01-01T00:00:00Z",
    power: float = 500.0,
    plevel_10: float = 400.0,
    plevel_90: float = 600.0,
) -> dict:
    return {
        "targetTime": target_time,
        "expectedPowerGenerationMegawatts": power,
        "plevels": {"plevel_10": plevel_10, "plevel_90": plevel_90},
    }


def _make_forecast_list(count: int) -> list[dict]:
    return [_make_forecast_value(target_time=f"2025-01-01T{i:02d}:00:00Z") for i in range(count)]


class TestCheckNationalForecast(unittest.TestCase):
    def _mock_call_api(self, return_value):
        return patch.object(_mod, "call_api", return_value=return_value)

    def test_passes_with_sufficient_data_and_correct_keys(self) -> None:
        min_count = int(2 * 24 * 2 + MIN_FORECAST_LEN * 2) + 1
        data = [
            {"targetTime": "t", "expectedPowerGenerationMegawatts": 100.0}
            for _ in range(min_count)
        ]
        with self._mock_call_api(data):
            _mod.check_national_forecast("token")

    def test_raises_when_data_too_short(self) -> None:
        with self._mock_call_api([{"targetTime": "t", "expectedPowerGenerationMegawatts": 1.0}]):
            with self.assertRaises(ValueError):
                _mod.check_national_forecast("token")

    def test_raises_when_key_missing(self) -> None:
        min_count = int(2 * 24 * 2 + MIN_FORECAST_LEN * 2) + 1
        data = [{"targetTime": "t"} for _ in range(min_count)]  # missing power key
        with self._mock_call_api(data):
            with self.assertRaises(ValueError):
                _mod.check_national_forecast("token")

    def test_url_includes_horizon_when_provided(self) -> None:
        min_count = int(2 * 24 * 2 + MIN_FORECAST_LEN * 2) + 1
        data = [{"targetTime": "t", "expectedPowerGenerationMegawatts": 1.0} for _ in range(min_count)]
        with patch.object(_mod, "call_api", return_value=data) as mock_call:
            _mod.check_national_forecast("token", horizon_minutes=60)
        call_url = mock_call.call_args.kwargs["url"]
        self.assertIn("forecast_horizon_minutes=60", call_url)

    def test_url_omits_horizon_when_not_provided(self) -> None:
        min_count = int(2 * 24 * 2 + MIN_FORECAST_LEN * 2) + 1
        data = [{"targetTime": "t", "expectedPowerGenerationMegawatts": 1.0} for _ in range(min_count)]
        with patch.object(_mod, "call_api", return_value=data) as mock_call:
            _mod.check_national_forecast("token")
        call_url = mock_call.call_args.kwargs["url"]
        self.assertNotIn("forecast_horizon_minutes", call_url)


class TestCheckNationalForecastMetadataTrueAndFalse(unittest.TestCase):
    def _make_values(self, power_map: dict[str, float]) -> list[dict]:
        return [{"targetTime": t, "expectedPowerGenerationMegawatts": p} for t, p in power_map.items()]

    def test_passes_when_values_are_identical(self) -> None:
        power_map = {"2025-01-01T00:00:00Z": 100.0, "2025-01-01T00:30:00Z": 200.0}
        data_true = {"forecastValues": self._make_values(power_map)}
        data_false = self._make_values(power_map)

        call_api_responses = [data_true, data_false]
        with patch.object(_mod, "call_api", side_effect=call_api_responses):
            _mod.check_national_forecast_metadata_true_and_false("token")

    def test_raises_when_values_differ(self) -> None:
        data_true = {
            "forecastValues": [{"targetTime": "2025-01-01T00:00:00Z", "expectedPowerGenerationMegawatts": 100.0}]
        }
        data_false = [{"targetTime": "2025-01-01T00:00:00Z", "expectedPowerGenerationMegawatts": 999.0}]
        with patch.object(_mod, "call_api", side_effect=[data_true, data_false]):
            with self.assertRaises(ValueError):
                _mod.check_national_forecast_metadata_true_and_false("token")


class TestCheckNationalForecastQuantilesOrder(unittest.TestCase):
    def _run_with_data(self, forecast_values: list[dict]) -> None:
        data = {"forecastValues": forecast_values}
        with patch.object(_mod, "call_api", return_value=data):
            _mod.check_national_forecast_quantiles_order("token")

    def test_passes_with_valid_quantile_order(self) -> None:
        self._run_with_data([_make_forecast_value(power=500, plevel_10=400, plevel_90=600)])

    def test_passes_when_all_values_below_100mw(self) -> None:
        # Low-power values are skipped regardless of quantile order
        self._run_with_data(
            [_make_forecast_value(power=50, plevel_10=90, plevel_90=10)]
        )

    def test_raises_when_expected_below_plevel_10_minus_buffer(self) -> None:
        # expected=100, plevel_10=200, buffer=50 → need expected >= 150 → 100 < 150 → fail
        with self.assertRaises(ValueError):
            self._run_with_data(
                [_make_forecast_value(power=100, plevel_10=200, plevel_90=300)]
            )

    def test_raises_when_plevels_missing(self) -> None:
        data = {"forecastValues": [{"targetTime": "t", "expectedPowerGenerationMegawatts": 500}]}
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_national_forecast_quantiles_order("token")

    def test_raises_when_forecast_values_empty(self) -> None:
        data = {"forecastValues": []}
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_national_forecast_quantiles_order("token")

    def test_passes_with_50mw_buffer_applied(self) -> None:
        # expected=160, plevel_10=200, plevel_90=300 → 160 >= 200-50=150 → passes
        self._run_with_data(
            [_make_forecast_value(power=160, plevel_10=200, plevel_90=300)]
        )


class TestCheckGspForecastAll(unittest.TestCase):
    def test_passes_with_valid_compact_response(self) -> None:
        min_count = int(2 * MIN_FORECAST_LEN) + 1
        data = [
            {"datetimeUtc": "t", "forecastValues": [{"gspId": i} for i in range(318)]}
            for _ in range(min_count)
        ]
        with patch.object(_mod, "call_api", return_value=data):
            _mod.check_gsp_forecast_all("token")

    def test_raises_when_too_few_datetimes(self) -> None:
        data = [{"datetimeUtc": "t", "forecastValues": [{"gspId": i} for i in range(318)]}]
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_gsp_forecast_all("token")

    def test_raises_when_too_few_gsp_values(self) -> None:
        min_count = int(2 * MIN_FORECAST_LEN) + 1
        # Only 300 GSP values — less than required 317
        data = [
            {"datetimeUtc": "t", "forecastValues": [{"gspId": i} for i in range(300)]}
            for _ in range(min_count)
        ]
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_gsp_forecast_all("token")


class TestCheckGspForecastAllCompactFalse(unittest.TestCase):
    def _make_compact_false_response(self, n_forecasts: int = 3, n_values: int = 70) -> dict:
        return {
            "forecasts": [
                {"forecastValues": [{"v": i} for i in range(n_values)]}
                for _ in range(n_forecasts)
            ]
        }

    def test_passes_with_valid_response(self) -> None:
        data = self._make_compact_false_response(n_forecasts=3, n_values=int(2 * MIN_FORECAST_LEN) + 1)
        with patch.object(_mod, "call_api", return_value=data):
            _mod.check_gsp_forecast_all_compact_false("token")

    def test_raises_when_forecast_count_not_three(self) -> None:
        data = self._make_compact_false_response(n_forecasts=2)
        with patch.object(_mod, "call_api", return_value=data):
            with self.assertRaises(ValueError):
                _mod.check_gsp_forecast_all_compact_false("token")


if __name__ == "__main__":
    unittest.main()
