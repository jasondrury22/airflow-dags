"""Unit tests for plugins/scripts/api_checks.py."""

import unittest
from unittest.mock import MagicMock, patch


class TestCheckLenGe(unittest.TestCase):
    def test_passes_when_equal(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        check_len_ge([1, 2, 3], 3)  # should not raise

    def test_passes_when_greater(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        check_len_ge([1, 2, 3, 4], 3)  # should not raise

    def test_raises_when_less(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        with self.assertRaises(ValueError):
            check_len_ge([1, 2], 3)

    def test_works_with_dict(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        check_len_ge({"a": 1, "b": 2}, 2)

    def test_works_with_float_min(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        check_len_ge([1, 2], 1.5)  # len=2 >= 1.5, should not raise

    def test_raises_with_float_min(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_ge
        with self.assertRaises(ValueError):
            check_len_ge([1], 1.5)  # len=1 < 1.5


class TestCheckLenEqual(unittest.TestCase):
    def test_passes_when_equal(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_equal
        check_len_equal([1, 2, 3], 3)

    def test_raises_when_too_short(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_equal
        with self.assertRaises(ValueError):
            check_len_equal([1, 2], 3)

    def test_raises_when_too_long(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_equal
        with self.assertRaises(ValueError):
            check_len_equal([1, 2, 3, 4], 3)

    def test_works_with_dict(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_len_equal
        check_len_equal({"a": 1}, 1)


class TestCheckKeyInData(unittest.TestCase):
    def test_passes_when_key_in_dict(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_key_in_data
        check_key_in_data({"foo": 1, "bar": 2}, "foo")

    def test_raises_when_key_missing_from_dict(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_key_in_data
        with self.assertRaises(ValueError):
            check_key_in_data({"foo": 1}, "bar")

    def test_passes_when_value_in_list(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_key_in_data
        check_key_in_data([1, 2, 3], 2)

    def test_raises_when_value_missing_from_list(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_key_in_data
        with self.assertRaises(ValueError):
            check_key_in_data([1, 2, 3], 4)

    def test_error_message_contains_key(self) -> None:
        from airflow_dags.plugins.scripts.api_checks import check_key_in_data
        with self.assertRaises(ValueError) as ctx:
            check_key_in_data({}, "missing_key")
        self.assertIn("missing_key", str(ctx.exception))


class TestGetBearerTokenFromAuth0(unittest.TestCase):
    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_returns_access_token(self, mock_requests: MagicMock) -> None:
        from airflow_dags.plugins.scripts.api_checks import get_bearer_token_from_auth0
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test-token-123"}
        mock_requests.post.return_value = mock_response

        token = get_bearer_token_from_auth0()

        self.assertEqual(token, "test-token-123")

    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_posts_to_correct_url(self, mock_requests: MagicMock) -> None:
        import airflow_dags.plugins.scripts.api_checks as api_checks
        from airflow_dags.plugins.scripts.api_checks import get_bearer_token_from_auth0
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "tok"}
        mock_requests.post.return_value = mock_response

        with patch.object(api_checks, "domain", "auth.example.com"):
            get_bearer_token_from_auth0()

        call_url = mock_requests.post.call_args[0][0]
        self.assertIn("auth.example.com", call_url)
        self.assertIn("/oauth/token", call_url)


class TestCallApi(unittest.TestCase):
    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_returns_json_on_200(self, mock_requests: MagicMock) -> None:
        from airflow_dags.plugins.scripts.api_checks import call_api
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"forecasts": [1, 2, 3]}
        mock_requests.get.return_value = mock_response

        result = call_api("https://example.com/api")

        self.assertEqual(result, {"forecasts": [1, 2, 3]})

    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_raises_on_non_200(self, mock_requests: MagicMock) -> None:
        from airflow_dags.plugins.scripts.api_checks import call_api
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_requests.get.return_value = mock_response

        with self.assertRaises(Exception) as ctx:
            call_api("https://example.com/api")
        self.assertIn("404", str(ctx.exception))

    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_sends_auth_header_when_token_provided(self, mock_requests: MagicMock) -> None:
        from airflow_dags.plugins.scripts.api_checks import call_api
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_requests.get.return_value = mock_response

        call_api("https://example.com/api", access_token="my-token")

        _, kwargs = mock_requests.get.call_args
        self.assertIn("Authorization", kwargs["headers"])
        self.assertIn("my-token", kwargs["headers"]["Authorization"])

    @patch("airflow_dags.plugins.scripts.api_checks.requests")
    def test_sends_no_auth_header_when_no_token(self, mock_requests: MagicMock) -> None:
        from airflow_dags.plugins.scripts.api_checks import call_api
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_requests.get.return_value = mock_response

        call_api("https://example.com/api")

        _, kwargs = mock_requests.get.call_args
        self.assertEqual(kwargs["headers"], {})


if __name__ == "__main__":
    unittest.main()
