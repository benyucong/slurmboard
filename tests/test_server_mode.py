import importlib.util
import io
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("slurmboard_server", ROOT / "slurmboard.py")
SLURMBOARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SLURMBOARD)


class ServerModeSelectionTests(unittest.TestCase):
    def test_auto_selects_server_without_slurm_commands(self):
        with mock.patch.object(SLURMBOARD, "_DASHBOARD_MODE", "auto"), \
             mock.patch.object(SLURMBOARD.shutil, "which", return_value=None):
            self.assertEqual(SLURMBOARD.dashboard_mode(), "server")

    def test_auto_selects_slurm_when_required_commands_exist(self):
        with mock.patch.object(SLURMBOARD, "_DASHBOARD_MODE", "auto"), \
             mock.patch.object(SLURMBOARD.shutil, "which", return_value="/usr/bin/tool"):
            self.assertEqual(SLURMBOARD.dashboard_mode(), "slurm")

    def test_explicit_mode_overrides_detection(self):
        with mock.patch.object(SLURMBOARD, "_DASHBOARD_MODE", "server"), \
             mock.patch.object(
                 SLURMBOARD.shutil, "which", side_effect=AssertionError("should not probe")
             ):
            self.assertEqual(SLURMBOARD.dashboard_mode(), "server")


class LinuxMetricParserTests(unittest.TestCase):
    def test_cpu_usage_from_proc_stat_samples(self):
        before = SLURMBOARD.parse_proc_stat("cpu  100 20 30 400 50 0 0 0\n")
        after = SLURMBOARD.parse_proc_stat("cpu  120 20 40 440 60 0 0 0\n")

        self.assertEqual(before, (600, 450))
        self.assertEqual(after, (680, 500))
        self.assertEqual(SLURMBOARD.cpu_percent_between(before, after), 37.5)

    def test_memory_uses_memavailable_and_reports_swap(self):
        result = SLURMBOARD.parse_meminfo(
            "MemTotal:       16000000 kB\n"
            "MemFree:         1000000 kB\n"
            "MemAvailable:    6000000 kB\n"
            "SwapTotal:       2000000 kB\n"
            "SwapFree:         500000 kB\n"
        )

        self.assertEqual(result["total"], 16_000_000 * 1024)
        self.assertEqual(result["used"], 10_000_000 * 1024)
        self.assertEqual(result["swap_used"], 1_500_000 * 1024)

    def test_disk_parser_keeps_real_mounts_and_filters_pseudo_mounts(self):
        result = SLURMBOARD.parse_df_output(
            "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
            "/dev/vda1 100000 40000 60000 40% /\n"
            "tmpfs 1000 10 990 1% /run\n"
            "/dev/vdb1 200000 50000 150000 25% /data\n"
            "proc 0 0 0 - /proc\n"
        )

        self.assertEqual([disk["mount"] for disk in result], ["/", "/data"])
        self.assertEqual(result[1]["used_percent"], 25.0)

    def test_nvidia_gpu_and_compute_process_parsers(self):
        gpus = SLURMBOARD.parse_nvidia_smi(
            "0, NVIDIA RTX 3090, GPU-aaa, 61, 73, 12000, 24576, 221.5, 350.0\n"
            "1, NVIDIA RTX 3090, GPU-bbb, N/A, 0, 18, 24576, [Not Supported], 350.0\n"
        )
        processes = SLURMBOARD.parse_gpu_processes(
            "GPU-aaa, 4567, python, 11800\nGPU-bbb, 7654, blender, N/A\n"
        )

        self.assertEqual(len(gpus), 2)
        self.assertEqual(gpus[0]["memory_total_mb"], 24576.0)
        self.assertIsNone(gpus[1]["temperature_c"])
        self.assertIsNone(gpus[1]["power_w"])
        self.assertEqual(processes[0]["pid"], 4567)
        self.assertEqual(processes[0]["command"], "python")

    def test_process_parser_uses_command_name_and_sorts_by_cpu(self):
        processes = SLURMBOARD.parse_ps_output(
            "11 alice 0.5 1.0 00:40 zsh\n"
            "22 alice 74.2 3.1 01:30 /opt/venv/bin/python3\n"
        )

        self.assertEqual([process["pid"] for process in processes], [22, 11])
        self.assertEqual(processes[0]["command"], "python3")
        self.assertNotIn("arguments", processes[0])


class ServerSnapshotTests(unittest.TestCase):
    def setUp(self):
        SLURMBOARD._CACHE.clear()

    def tearDown(self):
        SLURMBOARD._CACHE.clear()

    def test_collects_general_server_snapshot(self):
        reads = {
            "/proc/cpuinfo": "model name: Test CPU\n",
            "/proc/meminfo": "MemTotal: 1000 kB\nMemAvailable: 400 kB\n",
            "/proc/uptime": "7200.00 0.00\n",
        }
        proc_stats = iter(("cpu 10 0 0 90\n", "cpu 20 0 0 100\n"))

        def read_text(path):
            if path == "/proc/stat":
                return next(proc_stats)
            return reads.get(path, "")

        def command_output(command, timeout=10):
            if command[0] == "df":
                return "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/a 100 30 70 30% /\n"
            if command[0] == "ps":
                return "42 alice 8.5 1.2 00:10 python3\n"
            if "--query-gpu" in command[1]:
                return "0, Test GPU, GPU-1, 40, 25, 1024, 8192, 50, 200\n"
            return "GPU-1, 42, python3, 1024\n"

        with mock.patch.object(SLURMBOARD, "_read_text", side_effect=read_text), \
             mock.patch.object(SLURMBOARD, "_server_command", side_effect=command_output), \
             mock.patch.object(SLURMBOARD.shutil, "which", return_value="/usr/bin/nvidia-smi"), \
             mock.patch.object(SLURMBOARD.time, "sleep"), \
             mock.patch.object(SLURMBOARD.os, "getloadavg", return_value=(1.0, 0.5, 0.25)), \
             mock.patch.object(SLURMBOARD.os, "cpu_count", return_value=16), \
             mock.patch.object(SLURMBOARD.socket, "gethostname", return_value="gpu-box"), \
             mock.patch.object(SLURMBOARD.getpass, "getuser", return_value="alice"):
            snapshot = SLURMBOARD.collect_server_snapshot()

        self.assertEqual(snapshot["mode"], "server")
        self.assertEqual(snapshot["host"]["hostname"], "gpu-box")
        self.assertEqual(snapshot["cpu"]["model"], "Test CPU")
        self.assertEqual(snapshot["cpu"]["used_percent"], 50.0)
        self.assertEqual(snapshot["memory"]["used"], 600 * 1024)
        self.assertEqual(snapshot["gpus"][0]["name"], "Test GPU")
        self.assertEqual(snapshot["processes"][0]["command"], "python3")

    def test_server_page_and_refresh_endpoint(self):
        snapshot = {
            "mode": "server", "generated_at": "now",
            "host": {"hostname": "gpu-box", "user": "alice", "platform": "Linux",
                     "uptime_seconds": 1, "load": [0, 0, 0]},
            "cpu": {}, "memory": {}, "disks": [], "gpus": [],
            "gpu_processes": [], "processes": [], "errors": [],
        }
        with mock.patch.object(SLURMBOARD, "_DASHBOARD_MODE", "server"), \
             mock.patch.object(SLURMBOARD, "build_server_snapshot", return_value=snapshot):
            page = SLURMBOARD.render_page().decode("utf-8")

        self.assertIn("Server Dashboard", page)
        self.assertIn("/data/server?refresh=1", page)
        self.assertIn("gpu-box", page)

        handler = object.__new__(SLURMBOARD.Handler)
        handler.path = "/data/server?refresh=1"
        handler._send_json = mock.Mock()
        with mock.patch.object(SLURMBOARD, "build_server_snapshot", return_value=snapshot) as build:
            handler.do_GET()

        build.assert_called_once_with(refresh=True)
        handler._send_json.assert_called_once_with(200, snapshot)


if __name__ == "__main__":
    unittest.main()
