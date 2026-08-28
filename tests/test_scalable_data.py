import importlib.util
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("slurmboard_scalable", ROOT / "slurmboard.py")
SLURMBOARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SLURMBOARD)


class CompactSummaryTests(unittest.TestCase):
    def test_aggregates_partitions_without_double_counting_overlapping_nodes(self):
        output = (
            "gpu-a|up|1-00:00:00|2|idle|0/8/0/8|1000|gpu:a100:2|node[01-02]\n"
            "gpu-b|up|02:00:00|1|alloc|4/0/0/4|1000|gpu:a100:2|node02\n"
        )

        with mock.patch.object(SLURMBOARD, "_run", return_value=output) as run:
            summary, partitions = SLURMBOARD.collect_partition_summaries()

        self.assertEqual(run.call_count, 1)
        self.assertEqual(summary["node_count"], 2)
        self.assertEqual(summary["cpu_total"], 8)
        self.assertEqual(summary["gpu_total"], 4)
        self.assertIsNone(summary["gpu_alloc"])
        self.assertEqual(summary["gpu_by_type"]["a100"]["nodes"], 2)
        self.assertEqual([part["name"] for part in partitions], ["gpu-a", "gpu-b"])
        self.assertEqual(partitions[0]["gpu_total"], 4)
        self.assertEqual(partitions[1]["gpu_total"], 2)
        self.assertFalse(partitions[0]["nodes_loaded"])
        self.assertFalse(partitions[0]["jobs_loaded"])

    def test_initial_snapshot_does_not_query_personal_or_global_jobs(self):
        compact = {
            "generated_at": "now", "summary": {}, "partitions": [], "nodes": []
        }
        with mock.patch.object(
            SLURMBOARD, "build_cluster_summary", return_value=compact
        ), mock.patch.object(
            SLURMBOARD, "collect_active_queue", side_effect=AssertionError("queried queue")
        ), mock.patch.object(
            SLURMBOARD, "collect_user_jobs", side_effect=AssertionError("queried history")
        ):
            snapshot = SLURMBOARD.build_snapshot()

        self.assertFalse(snapshot["active_queue_loaded"])
        self.assertFalse(snapshot["user_jobs_loaded"])
        self.assertEqual(snapshot["active_queue"], [])


class LazyPartitionTests(unittest.TestCase):
    def tearDown(self):
        SLURMBOARD._PARTITION_NODELISTS.clear()

    def test_parses_exact_node_details_for_one_partition(self):
        output = (
            "nid000018 lumid mix 88/168/0/256 2048000 901120 "
            "gpu:a40:8,nvme:40000 gpu:a40:8(IDX:0-7),nvme:0\n"
            "nid000019 lumid idle 0/256/0/256 2048000 0 "
            "gpu:a40:8,nvme:40000 gpu:a40:0(IDX:N/A),nvme:0\n"
        )

        with mock.patch.object(SLURMBOARD, "_run", return_value=output) as run:
            nodes = SLURMBOARD.collect_partition_nodes("lumid")

        command = run.call_args.args[0]
        self.assertIn("-p", command)
        self.assertIn("lumid", command)
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0]["gpu_alloc"], 8)
        self.assertEqual(nodes[1]["gpu_idle"], 8)
        self.assertEqual(nodes[0]["mem_alloc_mb"], 901120)

    def test_reuses_compact_hostlist_for_scoped_scontrol_query(self):
        SLURMBOARD._PARTITION_NODELISTS["lumid"] = ("nid[000016-000023]",)
        output = (
            "NodeName=nid000016 State=IDLE CPUAlloc=0 CPUTot=256 CPULoad=0.10 "
            "Gres=gpu:a40:8 Partitions=lumid RealMemory=2048000 AllocMem=0 "
            "CfgTRES=cpu=256,gres/gpu=8,gres/gpu:a40=8 "
            "AllocTRES=cpu=0,gres/gpu=0,gres/gpu:a40=0\n"
        )

        with mock.patch.object(SLURMBOARD, "_run", return_value=output) as run:
            nodes = SLURMBOARD.collect_partition_nodes("lumid")

        command = run.call_args.args[0]
        self.assertEqual(command, [
            "scontrol", "-o", "show", "node", "nid[000016-000023]",
        ])
        self.assertEqual(nodes[0]["name"], "nid000016")
        self.assertEqual(nodes[0]["gpu_idle"], 8)
        self.assertEqual(nodes[0]["load"], 0.10)

    def test_partition_job_query_is_scoped(self):
        with mock.patch.object(SLURMBOARD, "_run", return_value="") as run:
            SLURMBOARD.collect_job_counts("standard-g")

        command = run.call_args.args[0]
        self.assertEqual(command[1:3], ["-p", "standard-g"])


class CacheTests(unittest.TestCase):
    def test_short_cache_reuses_value_until_forced(self):
        SLURMBOARD._CACHE.clear()
        loader = mock.Mock(side_effect=[{"value": 1}, {"value": 2}])

        first = SLURMBOARD._cached("sample", 30, loader)
        second = SLURMBOARD._cached("sample", 30, loader)
        refreshed = SLURMBOARD._cached("sample", 30, loader, refresh=True)

        self.assertIs(first, second)
        self.assertEqual(refreshed, {"value": 2})
        self.assertEqual(loader.call_count, 2)


class DashboardRefreshUITests(unittest.TestCase):
    def test_dashboard_offers_persistent_refresh_intervals(self):
        page = SLURMBOARD.PAGE_TEMPLATE

        self.assertIn('id="refresh-interval"', page)
        self.assertIn('<option value="0">Manual</option>', page)
        self.assertIn('<option value="60">1 minute</option>', page)
        self.assertIn('<option value="300">5 minutes</option>', page)
        self.assertIn("const DEFAULT_REFRESH_SECONDS = 60", page)
        self.assertIn("slurmboard_refresh_seconds", page)
        self.assertNotIn('onclick="location.reload()"', page)


if __name__ == "__main__":
    unittest.main()
