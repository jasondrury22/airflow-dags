"""Tests for DAG metadata: schedules, task IDs, dependencies, and trigger rules."""

import os
import unittest

from airflow.models import DagBag

DAGS_FOLDER = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../src/airflow_dags/dags"),
)

# Load DagBag once at module level — reused by all tests in this file.
_dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def _dag(dag_id: str):
    dag = _dagbag.dags.get(dag_id)
    if dag is None:
        raise KeyError(f"DAG '{dag_id}' not found in DagBag. Available: {list(_dagbag.dags)}")
    return dag


def _task_ids(dag_id: str) -> set[str]:
    return {t.task_id for t in _dag(dag_id).tasks}


class TestDagSchedules(unittest.TestCase):
    """Verify that each DAG runs on the expected cadence."""

    def _assert_schedule(self, dag_id: str, expected: str) -> None:
        dag = _dag(dag_id)
        self.assertEqual(str(dag.schedule_interval), expected, f"Schedule mismatch for {dag_id}")

    # UK forecast DAGs
    def test_uk_forecast_gsp_schedule(self) -> None:
        self._assert_schedule("uk-forecast-gsp", "15,45 * * * *")

    def test_uk_forecast_national_schedule(self) -> None:
        self._assert_schedule("uk-forecast-national", "12 */2 * * *")

    def test_uk_forecast_site_schedule(self) -> None:
        self._assert_schedule("uk-forecast-site", "*/15 * * * *")

    def test_uk_forecast_clouds_schedule(self) -> None:
        self._assert_schedule("uk-forecast-clouds", "12,42 * * * *")

    # UK consume DAGs
    def test_uk_consume_pvlive_intraday_schedule(self) -> None:
        self._assert_schedule("uk-consume-pvlive-intraday", "6,9,12,14,20,36,39,42,44,50 * * * *")

    def test_uk_consume_pvlive_dayafter_schedule(self) -> None:
        self._assert_schedule("uk-consume-pvlive-dayafter", "0 11 * * *")

    def test_uk_consume_sat_schedule(self) -> None:
        self._assert_schedule("uk-consume-sat", "*/5 * * * *")

    # UK analysis / management DAGs
    def test_uk_analysis_metrics_schedule(self) -> None:
        self._assert_schedule("uk-analysis-metrics", "0 21 * * *")

    def test_uk_analysis_metrics_me_schedule(self) -> None:
        self._assert_schedule("uk-analysis-metrics-me", "0 20 * * *")

    def test_uk_analysis_clouds_schedule(self) -> None:
        self._assert_schedule("uk-analysis-clouds", "0 6 * * *")

    def test_uk_manage_sitedb_cleanup_schedule(self) -> None:
        self._assert_schedule("uk-manage-sitedb-cleanup", "0 0 * * *")

    # India DAGs
    def test_india_consume_nwp_schedule(self) -> None:
        self._assert_schedule("india-consume-nwp", "0 * * * *")

    def test_india_consume_ruvnl_schedule(self) -> None:
        self._assert_schedule("india-consume-ruvnl", "*/3 * * * *")

    def test_india_consume_satellite_schedule(self) -> None:
        self._assert_schedule("india-consume-satellite", "*/5 * * * *")

    def test_india_forecast_ruvnl_schedule(self) -> None:
        self._assert_schedule("india-forecast-ruvnl", "0 * * * *")

    def test_india_forecast_ad_schedule(self) -> None:
        self._assert_schedule("india-forecast-ad", "*/15 * * * *")

    def test_india_api_check_schedule(self) -> None:
        self._assert_schedule("india-api-check", "0 * * * *")

    def test_india_manage_elb_reset_schedule(self) -> None:
        self._assert_schedule("india-manage-elb-reset", "0 0 1 * *")

    def test_india_manage_clean_up_logs_schedule(self) -> None:
        self._assert_schedule("india-manage-clean-up-logs", "0 3 * * *")


class TestDagCatchup(unittest.TestCase):
    """All DAGs must have catchup=False to avoid backfilling on deployment."""

    def test_all_dags_have_catchup_false(self) -> None:
        for dag_id, dag in _dagbag.dags.items():
            with self.subTest(dag_id=dag_id):
                self.assertFalse(dag.catchup, f"DAG '{dag_id}' has catchup=True")


class TestUkForecastGspStructure(unittest.TestCase):
    """uk-forecast-gsp has a specific failure-escalation task chain."""

    def test_expected_tasks_present(self) -> None:
        task_ids = _task_ids("uk-forecast-gsp")
        for expected in [
            "latest_only",
            "forecast-gsps",
            "check-forecast-gsps-last-run-subcritical",
            "check-forecast-gsps-last-run-critical",
            "blend-forecasts",
        ]:
            self.assertIn(expected, task_ids)

    def test_subcritical_check_triggers_on_failure(self) -> None:
        dag = _dag("uk-forecast-gsp")
        task = dag.get_task("check-forecast-gsps-last-run-subcritical")
        self.assertEqual(task.trigger_rule, "one_failed")

    def test_critical_check_triggers_on_failure(self) -> None:
        dag = _dag("uk-forecast-gsp")
        task = dag.get_task("check-forecast-gsps-last-run-critical")
        self.assertEqual(task.trigger_rule, "one_failed")

    def test_blend_forecasts_triggers_all_done(self) -> None:
        dag = _dag("uk-forecast-gsp")
        task = dag.get_task("blend-forecasts")
        self.assertEqual(task.trigger_rule, "all_done")

    def test_forecast_gsps_is_downstream_of_latest_only(self) -> None:
        dag = _dag("uk-forecast-gsp")
        forecast_task = dag.get_task("forecast-gsps")
        upstream_ids = {t.task_id for t in forecast_task.upstream_list}
        self.assertIn("latest_only", upstream_ids)

    def test_subcritical_is_downstream_of_forecast_gsps(self) -> None:
        dag = _dag("uk-forecast-gsp")
        subcritical_task = dag.get_task("check-forecast-gsps-last-run-subcritical")
        upstream_ids = {t.task_id for t in subcritical_task.upstream_list}
        self.assertIn("forecast-gsps", upstream_ids)

    def test_critical_is_downstream_of_subcritical(self) -> None:
        dag = _dag("uk-forecast-gsp")
        critical_task = dag.get_task("check-forecast-gsps-last-run-critical")
        upstream_ids = {t.task_id for t in critical_task.upstream_list}
        self.assertIn("check-forecast-gsps-last-run-subcritical", upstream_ids)


class TestUkConsumeSatStructure(unittest.TestCase):
    """uk-consume-sat: rss runs first; odegree only runs if rss fails."""

    def test_expected_tasks_present(self) -> None:
        task_ids = _task_ids("uk-consume-sat")
        for expected in ["latest-only", "consume-rss", "consume-odegree"]:
            self.assertIn(expected, task_ids)

    def test_odegree_triggers_on_all_failed(self) -> None:
        dag = _dag("uk-consume-sat")
        task = dag.get_task("consume-odegree")
        self.assertEqual(task.trigger_rule, "all_failed")

    def test_rss_is_downstream_of_latest_only(self) -> None:
        dag = _dag("uk-consume-sat")
        rss = dag.get_task("consume-rss")
        upstream_ids = {t.task_id for t in rss.upstream_list}
        self.assertIn("latest-only", upstream_ids)


class TestIndiaNwpConsumerStructure(unittest.TestCase):
    """india-consume-nwp has three parallel consumer tasks."""

    def test_expected_tasks_present(self) -> None:
        task_ids = _task_ids("india-consume-nwp")
        for expected in [
            "latest_only",
            "consume-ecmwf-nwp",
            "consume-gfs-nwp",
            "consume-metoffice-nwp",
        ]:
            self.assertIn(expected, task_ids)

    def test_consumers_are_downstream_of_latest_only(self) -> None:
        dag = _dag("india-consume-nwp")
        for consumer_id in ["consume-ecmwf-nwp", "consume-gfs-nwp", "consume-metoffice-nwp"]:
            consumer = dag.get_task(consumer_id)
            upstream_ids = {t.task_id for t in consumer.upstream_list}
            self.assertIn("latest_only", upstream_ids, f"{consumer_id} not downstream of latest_only")


class TestIndiaApiCheckStructure(unittest.TestCase):
    """india-api-check: get-bearer-token → sites → [forecast, generation]."""

    def test_expected_tasks_present(self) -> None:
        task_ids = _task_ids("india-api-check")
        for expected in [
            "check-api-get-bearer-token",
            "check-sites",
            "check-forecast",
            "check-generation",
        ]:
            self.assertIn(expected, task_ids)

    def test_sites_is_downstream_of_bearer_token(self) -> None:
        dag = _dag("india-api-check")
        sites = dag.get_task("check-sites")
        upstream_ids = {t.task_id for t in sites.upstream_list}
        self.assertIn("check-api-get-bearer-token", upstream_ids)

    def test_forecast_is_downstream_of_sites(self) -> None:
        dag = _dag("india-api-check")
        forecast = dag.get_task("check-forecast")
        upstream_ids = {t.task_id for t in forecast.upstream_list}
        self.assertIn("check-sites", upstream_ids)

    def test_generation_is_downstream_of_sites(self) -> None:
        dag = _dag("india-api-check")
        generation = dag.get_task("check-generation")
        upstream_ids = {t.task_id for t in generation.upstream_list}
        self.assertIn("check-sites", upstream_ids)


if __name__ == "__main__":
    unittest.main()
