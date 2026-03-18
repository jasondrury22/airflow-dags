"""Unit tests for plugins/callbacks/slack.py."""

import unittest
from unittest.mock import MagicMock, patch


class TestGetTaskLink(unittest.TestCase):
    def test_contains_url(self) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import get_task_link

        with patch.object(slack_module, "url", "airflow.example.com"):
            link = get_task_link()

        self.assertIn("airflow.example.com", link)

    def test_contains_dag_id_template(self) -> None:
        from airflow_dags.plugins.callbacks.slack import get_task_link

        link = get_task_link()
        # After f-string processing, Airflow Jinja template braces {{ }} become { }
        self.assertIn("ti.dag_id", link)

    def test_contains_task_id_template(self) -> None:
        from airflow_dags.plugins.callbacks.slack import get_task_link

        link = get_task_link()
        self.assertIn("ti.task_id", link)


class TestBuildMessage(unittest.TestCase):
    def test_critical_message_contains_runbook_reference(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency, _build_message

        msg = _build_message("link", "🇬🇧", Urgency.CRITICAL)

        self.assertIn("run book", msg)
        self.assertIn("link", msg)
        self.assertIn("🇬🇧", msg)

    def test_subcritical_message_contains_no_support_notice(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency, _build_message

        msg = _build_message("link", "🇮🇳", Urgency.SUBCRITICAL)

        self.assertIn("No out of hours", msg)
        self.assertIn("link", msg)
        self.assertIn("🇮🇳", msg)

    def test_additional_context_included_in_message(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency, _build_message

        msg = _build_message("link", "🏳️", Urgency.CRITICAL, "Extra context here.")

        self.assertIn("Extra context here.", msg)

    def test_critical_uses_error_emoji(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency, _build_message

        msg = _build_message("link", "", Urgency.CRITICAL)
        self.assertIn("❌", msg)

    def test_subcritical_uses_warning_emoji(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency, _build_message

        msg = _build_message("link", "", Urgency.SUBCRITICAL)
        self.assertIn("⚠️", msg)


class TestGetSlackMessageCallback(unittest.TestCase):
    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_production_critical_uses_dedicated_channel(
        self, mock_send: MagicMock
    ) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import Urgency, get_slack_message_callback

        with patch.object(slack_module, "env", "production"):
            get_slack_message_callback(country="gb", urgency=Urgency.CRITICAL)

        _, kwargs = mock_send.call_args
        self.assertIn("production", kwargs["channel"])
        self.assertIn("critical", kwargs["channel"])

    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_production_subcritical_uses_dedicated_channel(
        self, mock_send: MagicMock
    ) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import Urgency, get_slack_message_callback

        with patch.object(slack_module, "env", "production"):
            get_slack_message_callback(country="gb", urgency=Urgency.SUBCRITICAL)

        _, kwargs = mock_send.call_args
        self.assertIn("subcritical", kwargs["channel"])

    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_non_production_uses_single_channel(self, mock_send: MagicMock) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import Urgency, get_slack_message_callback

        with patch.object(slack_module, "env", "development"):
            get_slack_message_callback(country="gb", urgency=Urgency.CRITICAL)

        _, kwargs = mock_send.call_args
        channel = kwargs["channel"]
        self.assertIn("development", channel)
        self.assertNotIn("critical", channel)

    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_unknown_country_uses_default_flag(self, mock_send: MagicMock) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import (
            DEFAULT_FLAG,
            Urgency,
            get_slack_message_callback,
        )

        with patch.object(slack_module, "env", "development"):
            get_slack_message_callback(country="zz", urgency=Urgency.CRITICAL)

        _, kwargs = mock_send.call_args
        self.assertIn(DEFAULT_FLAG, kwargs["text"])

    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_known_country_uses_correct_flag(self, mock_send: MagicMock) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import FLAGS, Urgency, get_slack_message_callback

        with patch.object(slack_module, "env", "development"):
            get_slack_message_callback(country="nl", urgency=Urgency.CRITICAL)

        _, kwargs = mock_send.call_args
        self.assertIn(FLAGS["nl"], kwargs["text"])

    @patch("airflow_dags.plugins.callbacks.slack.send_slack_notification")
    def test_country_code_is_case_insensitive(self, mock_send: MagicMock) -> None:
        import airflow_dags.plugins.callbacks.slack as slack_module
        from airflow_dags.plugins.callbacks.slack import FLAGS, Urgency, get_slack_message_callback

        with patch.object(slack_module, "env", "development"):
            get_slack_message_callback(country="GB", urgency=Urgency.CRITICAL)

        _, kwargs = mock_send.call_args
        self.assertIn(FLAGS["gb"], kwargs["text"])


class TestUrgencyEnum(unittest.TestCase):
    def test_urgency_values(self) -> None:
        from airflow_dags.plugins.callbacks.slack import Urgency

        self.assertEqual(Urgency.CRITICAL, "critical")
        self.assertEqual(Urgency.SUBCRITICAL, "subcritical")


if __name__ == "__main__":
    unittest.main()
