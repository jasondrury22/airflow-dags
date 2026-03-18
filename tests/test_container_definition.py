"""Unit tests for ContainerDefinition in plugins/operators/ecs_run_task_operator.py."""

import unittest
from unittest.mock import patch


def _make_container(**kwargs):  # type: ignore[no-untyped-def]
    from airflow_dags.plugins.operators.ecs_run_task_operator import ContainerDefinition

    defaults = {
        "name": "test-container",
        "container_image": "ghcr.io/example/app",
        "container_tag": "1.0.0",
    }
    return ContainerDefinition(**{**defaults, **kwargs})


class TestContainerDefinitionValidation(unittest.TestCase):
    def test_valid_defaults_do_not_raise(self) -> None:
        _make_container()  # should not raise

    def test_invalid_cpu_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _make_container(container_cpu=999)
        self.assertIn("CPU", str(ctx.exception))

    def test_invalid_memory_for_cpu_raises(self) -> None:
        # cpu=1024 requires memory in range(2048, 9216, 1024)
        with self.assertRaises(ValueError) as ctx:
            _make_container(container_cpu=1024, container_memory=512)
        self.assertIn("Memory", str(ctx.exception))

    def test_valid_cpu_memory_combination(self) -> None:
        _make_container(container_cpu=2048, container_memory=4096)  # should not raise

    def test_invalid_domain_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _make_container(domain="france")
        self.assertIn("Domain", str(ctx.exception))

    def test_valid_domains(self) -> None:
        for domain in ["uk", "india", "nl"]:
            with self.subTest(domain=domain):
                _make_container(domain=domain)  # should not raise

    def test_storage_below_minimum_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _make_container(container_storage=10)
        self.assertIn("Storage", str(ctx.exception))

    def test_storage_at_minimum_does_not_raise(self) -> None:
        _make_container(container_storage=20)  # should not raise


class TestClusterRegionTuple(unittest.TestCase):
    def test_india_returns_india_cluster_and_ap_region(self) -> None:
        cd = _make_container(domain="india")
        with patch.dict("os.environ", {"ENVIRONMENT": "production"}):
            cluster, region = cd.cluster_region_tuple
        self.assertIn("india", cluster)
        self.assertEqual(region, "ap-south-1")

    def test_uk_returns_nowcasting_cluster_and_eu_region(self) -> None:
        cd = _make_container(domain="uk")
        with patch.dict("os.environ", {"ENVIRONMENT": "development"}):
            cluster, region = cd.cluster_region_tuple
        self.assertIn("Nowcasting", cluster)
        self.assertEqual(region, "eu-west-1")

    def test_nl_returns_nowcasting_cluster_and_eu_region(self) -> None:
        cd = _make_container(domain="nl")
        with patch.dict("os.environ", {"ENVIRONMENT": "development"}):
            cluster, region = cd.cluster_region_tuple
        self.assertIn("Nowcasting", cluster)
        self.assertEqual(region, "eu-west-1")

    def test_cluster_name_includes_environment(self) -> None:
        cd = _make_container(domain="uk")
        with patch.dict("os.environ", {"ENVIRONMENT": "production"}):
            cluster, _ = cd.cluster_region_tuple
        self.assertIn("production", cluster)


class TestEcsEnvironment(unittest.TestCase):
    def test_returns_list_of_name_value_dicts(self) -> None:
        cd = _make_container()
        env = cd.ecs_environment()
        self.assertIsInstance(env, list)
        for item in env:
            self.assertIn("name", item)
            self.assertIn("value", item)

    def test_includes_default_aws_region(self) -> None:
        cd = _make_container()
        with patch.dict("os.environ", {"AWS_DEFAULT_REGION": "eu-west-2"}):
            env = cd.ecs_environment()
        names = {e["name"]: e["value"] for e in env}
        self.assertEqual(names["AWS_REGION"], "eu-west-2")

    def test_includes_custom_env_vars(self) -> None:
        cd = _make_container(container_env={"MY_VAR": "hello"})
        env = cd.ecs_environment()
        names = {e["name"]: e["value"] for e in env}
        self.assertEqual(names["MY_VAR"], "hello")

    def test_default_env_keys_always_present(self) -> None:
        cd = _make_container()
        env = cd.ecs_environment()
        names = {e["name"] for e in env}
        self.assertIn("AWS_REGION", names)
        self.assertIn("SENTRY_DSN", names)
        self.assertIn("ENVIRONMENT", names)


class TestEcsSecrets(unittest.TestCase):
    def test_empty_when_no_secrets_configured(self) -> None:
        cd = _make_container()
        self.assertEqual(cd.ecs_secrets(), [])

    def test_returns_arn_format(self) -> None:
        cd = _make_container(
            domain="uk",
            container_secret_env={"my-secret": ["SECRET_KEY"]},
        )
        with patch.dict("os.environ", {"AWS_OWNER_ID": "123456789"}):
            secrets = cd.ecs_secrets()

        self.assertEqual(len(secrets), 1)
        entry = secrets[0]
        self.assertEqual(entry["name"], "SECRET_KEY")
        self.assertIn("arn:aws:secretsmanager", entry["valueFrom"])
        self.assertIn("my-secret", entry["valueFrom"])
        self.assertIn("SECRET_KEY", entry["valueFrom"])
        self.assertIn("123456789", entry["valueFrom"])

    def test_multiple_keys_from_same_secret(self) -> None:
        cd = _make_container(
            container_secret_env={"my-secret": ["KEY_A", "KEY_B"]},
        )
        secrets = cd.ecs_secrets()
        self.assertEqual(len(secrets), 2)
        names = {s["name"] for s in secrets}
        self.assertEqual(names, {"KEY_A", "KEY_B"})


class TestEcsContainerDefinition(unittest.TestCase):
    def test_image_combines_repo_and_tag(self) -> None:
        cd = _make_container(
            container_image="ghcr.io/example/app",
            container_tag="2.3.1",
        )
        defn = cd.ecs_container_definition()
        self.assertEqual(defn["image"], "ghcr.io/example/app:2.3.1")

    def test_essential_is_true(self) -> None:
        defn = _make_container().ecs_container_definition()
        self.assertTrue(defn["essential"])

    def test_name_matches_container_name(self) -> None:
        cd = _make_container(name="my-task")
        defn = cd.ecs_container_definition()
        self.assertEqual(defn["name"], "my-task")

    def test_log_configuration_uses_awslogs(self) -> None:
        defn = _make_container().ecs_container_definition()
        self.assertEqual(defn["logConfiguration"]["logDriver"], "awslogs")

    def test_command_included_in_definition(self) -> None:
        cd = _make_container(container_command=["python", "main.py"])
        defn = cd.ecs_container_definition()
        self.assertEqual(defn["command"], ["python", "main.py"])


class TestEcsRegisterTaskKwargs(unittest.TestCase):
    def test_cpu_and_memory_are_strings(self) -> None:
        cd = _make_container(container_cpu=1024, container_memory=2048)
        kwargs = cd.ecs_register_task_kwargs()
        self.assertEqual(kwargs["cpu"], "1024")
        self.assertEqual(kwargs["memory"], "2048")

    def test_fargate_compatibility_required(self) -> None:
        kwargs = _make_container().ecs_register_task_kwargs()
        self.assertIn("FARGATE", kwargs["requiresCompatibilities"])

    def test_network_mode_is_awsvpc(self) -> None:
        kwargs = _make_container().ecs_register_task_kwargs()
        self.assertEqual(kwargs["networkMode"], "awsvpc")

    def test_tags_include_name_and_domain(self) -> None:
        cd = _make_container(name="my-task", domain="india")
        kwargs = cd.ecs_register_task_kwargs()
        tag_map = {t["key"]: t["value"] for t in kwargs["tags"]}
        self.assertEqual(tag_map["name"], "my-task")
        self.assertEqual(tag_map["domain"], "india")

    def test_ephemeral_storage_included_when_above_20gb(self) -> None:
        cd = _make_container(container_storage=50)
        kwargs = cd.ecs_register_task_kwargs()
        self.assertIn("ephemeralStorage", kwargs)
        self.assertEqual(kwargs["ephemeralStorage"]["sizeInGiB"], 50)

    def test_ephemeral_storage_omitted_at_20gb(self) -> None:
        cd = _make_container(container_storage=20)
        kwargs = cd.ecs_register_task_kwargs()
        self.assertNotIn("ephemeralStorage", kwargs)


if __name__ == "__main__":
    unittest.main()
