"""Unit tests for plugins/scripts/elastic_beanstalk.py."""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch


def _make_eb_ec2_mocks(
    instance_launch_times: dict[str, datetime],
) -> tuple[MagicMock, MagicMock]:
    """Build eb and ec2 mocks pre-configured with the given instance launch times."""
    eb = MagicMock()
    ec2 = MagicMock()

    eb.describe_environment_resources.return_value = {
        "EnvironmentResources": {"Instances": [{"Id": id_} for id_ in instance_launch_times]},
    }
    ec2.describe_instances.side_effect = lambda InstanceIds: {
        "Reservations": [{"Instances": [{"LaunchTime": instance_launch_times[InstanceIds[0]]}]}],
    }
    return eb, ec2


class TestTerminateAnyOldInstances(unittest.TestCase):
    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_returns_early_with_single_instance(self, mock_boto3: MagicMock) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import terminate_any_old_instances

        eb = MagicMock()
        ec2 = MagicMock()
        mock_boto3.client.side_effect = lambda svc: eb if svc == "elasticbeanstalk" else ec2
        eb.describe_environment_resources.return_value = {
            "EnvironmentResources": {"Instances": [{"Id": "i-only"}]},
        }

        terminate_any_old_instances("my-env", sleep_seconds=0)

        ec2.terminate_instances.assert_not_called()

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_returns_early_with_zero_instances(self, mock_boto3: MagicMock) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import terminate_any_old_instances

        eb = MagicMock()
        ec2 = MagicMock()
        mock_boto3.client.side_effect = lambda svc: eb if svc == "elasticbeanstalk" else ec2
        eb.describe_environment_resources.return_value = {
            "EnvironmentResources": {"Instances": []},
        }

        terminate_any_old_instances("my-env", sleep_seconds=0)

        ec2.terminate_instances.assert_not_called()

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_terminates_older_instance_keeps_youngest(self, mock_boto3: MagicMock) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import terminate_any_old_instances

        launch_times = {
            "i-old": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "i-new": datetime(2024, 1, 2, tzinfo=timezone.utc),
        }
        eb, ec2 = _make_eb_ec2_mocks(launch_times)
        mock_boto3.client.side_effect = lambda svc: eb if svc == "elasticbeanstalk" else ec2

        terminate_any_old_instances("my-env", sleep_seconds=0)

        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-old"])

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_youngest_of_three_is_spared(self, mock_boto3: MagicMock) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import terminate_any_old_instances

        launch_times = {
            "i-1": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "i-2": datetime(2024, 1, 3, tzinfo=timezone.utc),  # youngest
            "i-3": datetime(2024, 1, 2, tzinfo=timezone.utc),
        }
        eb, ec2 = _make_eb_ec2_mocks(launch_times)
        mock_boto3.client.side_effect = lambda svc: eb if svc == "elasticbeanstalk" else ec2

        terminate_any_old_instances("my-env", sleep_seconds=0)

        terminated = {c.kwargs["InstanceIds"][0] for c in ec2.terminate_instances.call_args_list}
        self.assertNotIn("i-2", terminated)
        self.assertIn("i-1", terminated)
        self.assertIn("i-3", terminated)

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.time")
    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_sleeps_between_terminations(
        self, mock_boto3: MagicMock, mock_time: MagicMock
    ) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import terminate_any_old_instances

        launch_times = {
            "i-old": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "i-new": datetime(2024, 1, 2, tzinfo=timezone.utc),
        }
        eb, ec2 = _make_eb_ec2_mocks(launch_times)
        mock_boto3.client.side_effect = lambda svc: eb if svc == "elasticbeanstalk" else ec2

        terminate_any_old_instances("my-env", sleep_seconds=60)

        mock_time.sleep.assert_called_once_with(60)


class TestScaleElasticBeanstalkInstance(unittest.TestCase):
    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_calls_update_environment_with_correct_args(self, mock_boto3: MagicMock) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import scale_elastic_beanstalk_instance

        eb = MagicMock()
        mock_boto3.client.return_value = eb

        scale_elastic_beanstalk_instance("my-env", number_of_instances=3)

        eb.update_environment.assert_called_once_with(
            EnvironmentName="my-env",
            OptionSettings=[
                {
                    "Namespace": "aws:autoscaling:asg",
                    "OptionName": "MinSize",
                    "Value": "3",
                },
                {
                    "Namespace": "aws:autoscaling:asg",
                    "OptionName": "MaxSize",
                    "Value": "3",
                },
            ],
        )

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.time")
    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_sleeps_when_sleep_seconds_nonzero(
        self, mock_boto3: MagicMock, mock_time: MagicMock
    ) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import scale_elastic_beanstalk_instance

        mock_boto3.client.return_value = MagicMock()

        scale_elastic_beanstalk_instance("my-env", number_of_instances=1, sleep_seconds=30)

        mock_time.sleep.assert_called_once_with(30)

    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.time")
    @patch("airflow_dags.plugins.scripts.elastic_beanstalk.boto3")
    def test_does_not_sleep_by_default(
        self, mock_boto3: MagicMock, mock_time: MagicMock
    ) -> None:
        from airflow_dags.plugins.scripts.elastic_beanstalk import scale_elastic_beanstalk_instance

        mock_boto3.client.return_value = MagicMock()

        scale_elastic_beanstalk_instance("my-env", number_of_instances=2)

        mock_time.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
