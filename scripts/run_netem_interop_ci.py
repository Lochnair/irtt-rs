#!/usr/bin/env python3
"""Run local netem-based IRTT client interop comparisons on Linux.

The harness creates two network namespaces connected by a veth pair, runs an
upstream irtt server in the server namespace, and runs the existing paired
client comparison harness in the client namespace.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import shutil
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPARE_SCRIPT = REPO_ROOT / "scripts" / "compare_irtt_clients.py"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "target" / "interop-netem"
CLIENT_NS = "irtt-client"
SERVER_NS = "irtt-server"
CLIENT_VETH = "veth-client"
SERVER_VETH = "veth-server"
CLIENT_ADDR = "10.10.0.1/24"
SERVER_ADDR = "10.10.0.2/24"
SERVER_TARGET = "10.10.0.2:2112"


@dataclass(frozen=True)
class Scenario:
    name: str
    duration: str
    interval: str
    client_to_server: str
    server_to_client: str
    expected_rtt_us: float | None
    mode: str


SCENARIOS = {
    scenario.name: scenario
    for scenario in [
        Scenario("baseline-no-netem", "5s", "100ms", "", "", None, "deterministic"),
        Scenario("symmetric-20ms", "5s", "100ms", "delay 20ms", "delay 20ms", 40_000, "deterministic"),
        Scenario(
            "symmetric-50ms-jitter",
            "5s",
            "100ms",
            "delay 50ms 5ms",
            "delay 50ms 5ms",
            100_000,
            "jitter",
        ),
        Scenario("asymmetric-delay", "5s", "100ms", "delay 40ms", "delay 10ms", 50_000, "deterministic"),
        Scenario("packet-loss", "10s", "50ms", "loss 5%", "loss 5%", None, "loss"),
    ]
}


class CommandError(RuntimeError):
    def __init__(self, command: list[str], returncode: int, stdout: str, stderr: str):
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"{shlex.join(command)} exited {returncode}")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run upstream irtt vs irtt-rs comparisons inside netem namespaces."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS),
        help="Scenario to run. Repeatable. Defaults to baseline-no-netem unless --all is set.",
    )
    group.add_argument("--all", action="store_true", help="Run all netem scenarios.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Artifact root directory (default: {DEFAULT_OUTPUT_ROOT}).",
    )
    parser.add_argument(
        "--upstream-irtt",
        default="irtt",
        help="Path to upstream irtt executable (default: irtt from PATH).",
    )
    parser.add_argument(
        "--irtt-rs-command",
        default=str(REPO_ROOT / "target" / "debug" / "irtt-cli"),
        help="Path or command for irtt-rs client (default: target/debug/irtt-cli).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Do not run cargo build -p irtt-cli before comparisons.",
    )
    parser.add_argument(
        "--sudo",
        default="sudo",
        help="Privilege escalation command for ip/tc namespace operations (default: sudo).",
    )
    parser.add_argument(
        "--mean-abs-tolerance-us",
        type=float,
        default=2_000.0,
        help="Minimum deterministic mean RTT delta tolerance in microseconds.",
    )
    parser.add_argument(
        "--mean-pct-tolerance",
        type=float,
        default=0.05,
        help="Deterministic mean RTT delta tolerance as a fraction of expected RTT.",
    )
    parser.add_argument(
        "--jitter-mean-abs-tolerance-us",
        type=float,
        default=8_000.0,
        help="Minimum jitter scenario mean RTT delta fail tolerance in microseconds.",
    )
    parser.add_argument(
        "--loss-rate-warning-pct",
        type=float,
        default=10.0,
        help="Packet-loss scenario warning threshold for client loss-rate delta.",
    )
    parser.add_argument(
        "--loss-abs-tolerance-pct",
        type=float,
        default=8.0,
        help="Packet-loss scenario absolute tolerance from expected RTT loss percentage.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected = select_scenarios(args)
    run_dir = Path(args.output_dir) / dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run-info.json").write_text(
        json.dumps(
            {
                "created_at": utc_now(),
                "scenarios": [scenario.name for scenario in selected],
                "client_namespace": CLIENT_NS,
                "server_namespace": SERVER_NS,
                "target": SERVER_TARGET,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        preflight(args)
        upstream_irtt = resolve_command(args.upstream_irtt)
        irtt_rs_command = resolve_irtt_rs_command(args.irtt_rs_command)
        if not args.skip_build:
            run_checked(["cargo", "build", "-p", "irtt-cli"], run_dir / "cargo-build")

        cleanup(args, quiet=True)
        setup_namespaces(args, run_dir)
        server_proc = start_server(args, upstream_irtt, run_dir)
        try:
            wait_for_server(args, upstream_irtt, run_dir)
            results = []
            all_ok = True
            for scenario in selected:
                result = run_scenario(args, scenario, upstream_irtt, irtt_rs_command, run_dir)
                results.append(result)
                all_ok = all_ok and result["classification"] != "fail"
            write_run_summary(run_dir, results)
            return 0 if all_ok else 1
        finally:
            stop_process(server_proc)
    except Exception as exc:
        (run_dir / "error.txt").write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        cleanup(args, quiet=True)
        print(f"[{utc_now()}] wrote netem artifacts to {run_dir}", flush=True)


def select_scenarios(args: argparse.Namespace) -> list[Scenario]:
    if args.all:
        return [SCENARIOS[name] for name in sorted(SCENARIOS)]
    names = args.scenario or ["baseline-no-netem"]
    return [SCENARIOS[name] for name in names]


def preflight(args: argparse.Namespace) -> None:
    if platform.system() != "Linux":
        raise RuntimeError("netem interop requires Linux network namespaces and tc netem")
    for binary in ["ip", "tc"]:
        if shutil.which(binary) is None:
            raise RuntimeError(f"required command not found in PATH: {binary}")
    if args.sudo and shutil.which(args.sudo) is None:
        raise RuntimeError(f"sudo command not found in PATH: {args.sudo}")
    if not COMPARE_SCRIPT.exists():
        raise RuntimeError(f"missing comparison harness: {COMPARE_SCRIPT}")


def resolve_command(command: str) -> str:
    parts = shlex.split(command)
    if len(parts) != 1:
        raise RuntimeError(f"expected a single executable path, got: {command}")
    executable = parts[0]
    if os.sep in executable:
        return str(Path(executable).resolve())
    resolved = shutil.which(executable)
    if resolved is None:
        raise RuntimeError(f"executable not found in PATH: {executable}")
    return resolved


def resolve_irtt_rs_command(command: str) -> str:
    parts = shlex.split(command)
    if not parts:
        raise RuntimeError("--irtt-rs-command must not be empty")
    executable = parts[0]
    if os.sep in executable:
        parts[0] = str(Path(executable).resolve())
    else:
        resolved = shutil.which(executable)
        if resolved is not None:
            parts[0] = resolved
    return shlex.join(parts)


def sudo_prefix(args: argparse.Namespace) -> list[str]:
    return [args.sudo] if args.sudo else []


def netns(args: argparse.Namespace, namespace: str, command: list[str]) -> list[str]:
    return [*sudo_prefix(args), "ip", "netns", "exec", namespace, *command]


def run_checked(
    command: list[str],
    log_prefix: Path,
    check: bool = True,
    ignored_stderr_texts: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    print(f"[{utc_now()}] {shlex.join(command)}", flush=True)
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stderr = filter_stderr(completed.stderr, ignored_stderr_texts or [])
    log_prefix.parent.mkdir(parents=True, exist_ok=True)
    (log_prefix.with_suffix(".stdout")).write_text(completed.stdout, encoding="utf-8")
    if stderr or not ignored_stderr_texts:
        (log_prefix.with_suffix(".stderr")).write_text(stderr, encoding="utf-8")
    (log_prefix.with_suffix(".command")).write_text(shlex.join(command) + "\n", encoding="utf-8")
    if check and completed.returncode != 0:
        raise CommandError(command, completed.returncode, completed.stdout, stderr)
    return completed


def filter_stderr(stderr: str, ignored_texts: list[str]) -> str:
    if not stderr or not ignored_texts:
        return stderr
    kept = [line for line in stderr.splitlines() if not any(text in line for text in ignored_texts)]
    if stderr.endswith("\n") and kept:
        return "\n".join(kept) + "\n"
    return "\n".join(kept)


def setup_namespaces(args: argparse.Namespace, run_dir: Path) -> None:
    commands = [
        [*sudo_prefix(args), "ip", "netns", "add", CLIENT_NS],
        [*sudo_prefix(args), "ip", "netns", "add", SERVER_NS],
        [*sudo_prefix(args), "ip", "link", "add", CLIENT_VETH, "type", "veth", "peer", "name", SERVER_VETH],
        [*sudo_prefix(args), "ip", "link", "set", CLIENT_VETH, "netns", CLIENT_NS],
        [*sudo_prefix(args), "ip", "link", "set", SERVER_VETH, "netns", SERVER_NS],
        [*sudo_prefix(args), "ip", "-n", CLIENT_NS, "addr", "add", CLIENT_ADDR, "dev", CLIENT_VETH],
        [*sudo_prefix(args), "ip", "-n", SERVER_NS, "addr", "add", SERVER_ADDR, "dev", SERVER_VETH],
        [*sudo_prefix(args), "ip", "-n", CLIENT_NS, "link", "set", "lo", "up"],
        [*sudo_prefix(args), "ip", "-n", SERVER_NS, "link", "set", "lo", "up"],
        [*sudo_prefix(args), "ip", "-n", CLIENT_NS, "link", "set", CLIENT_VETH, "up"],
        [*sudo_prefix(args), "ip", "-n", SERVER_NS, "link", "set", SERVER_VETH, "up"],
    ]
    for index, command in enumerate(commands):
        run_checked(command, run_dir / "setup" / f"{index:02d}")


def start_server(args: argparse.Namespace, upstream_irtt: str, run_dir: Path) -> subprocess.Popen[bytes]:
    command = netns(
        args,
        SERVER_NS,
        [upstream_irtt, "server", "-b", SERVER_TARGET, "--tstamp=dual"],
    )
    (run_dir / "server.command").write_text(shlex.join(command) + "\n", encoding="utf-8")
    stdout = (run_dir / "server.stdout").open("wb")
    stderr = (run_dir / "server.stderr").open("wb")
    print(f"[{utc_now()}] starting server: {shlex.join(command)}", flush=True)
    try:
        return subprocess.Popen(command, cwd=REPO_ROOT, stdout=stdout, stderr=stderr)
    except Exception:
        stdout.close()
        stderr.close()
        raise


def wait_for_server(args: argparse.Namespace, upstream_irtt: str, run_dir: Path) -> None:
    command = netns(
        args,
        CLIENT_NS,
        [upstream_irtt, "client", "-n", "-Q", "--timeouts=1s", SERVER_TARGET],
    )
    deadline = time.monotonic() + 10.0
    attempt = 0
    last: subprocess.CompletedProcess[str] | None = None
    while time.monotonic() < deadline:
        attempt += 1
        last = run_checked(command, run_dir / "server-ready" / f"attempt-{attempt:02d}", check=False)
        if last.returncode == 0:
            return
        time.sleep(0.5)
    details = ""
    if last is not None:
        details = (last.stderr or last.stdout).strip()
    raise RuntimeError(f"upstream irtt server did not become reachable: {details}")


def run_scenario(
    args: argparse.Namespace,
    scenario: Scenario,
    upstream_irtt: str,
    irtt_rs_command: str,
    run_dir: Path,
) -> dict[str, Any]:
    scenario_dir = run_dir / scenario.name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    apply_netem(args, scenario, scenario_dir)
    compare_root = scenario_dir / "comparison"
    command = [
        sys.executable,
        str(COMPARE_SCRIPT),
        "--target",
        SERVER_TARGET,
        "--duration",
        scenario.duration,
        "--interval",
        scenario.interval,
        "--output-dir",
        str(compare_root),
        "--upstream-irtt",
        upstream_irtt,
        "--irtt-rs-command",
        irtt_rs_command,
        "--client-command-prefix",
        shlex.join(netns(args, CLIENT_NS, [])),
        "--skip-build",
    ]
    completed = run_checked(command, scenario_dir / "compare", check=False)
    comparison = load_latest_comparison(compare_root)
    result = classify_scenario(args, scenario, completed.returncode, comparison)
    result.update(
        {
            "scenario": scenario.__dict__,
            "qdisc": {
                "client_to_server": scenario.client_to_server or "none",
                "server_to_client": scenario.server_to_client or "none",
            },
            "comparison_returncode": completed.returncode,
            "comparison_root": str(compare_root),
            "comparison_json": comparison.get("_path"),
        }
    )
    write_json(scenario_dir / "netem-summary.json", result)
    write_scenario_markdown(scenario_dir / "summary.md", result)
    return result


def apply_netem(args: argparse.Namespace, scenario: Scenario, scenario_dir: Path) -> None:
    delete_qdiscs(args, scenario_dir)
    commands: list[tuple[str, str, str]] = [
        (CLIENT_NS, CLIENT_VETH, scenario.client_to_server),
        (SERVER_NS, SERVER_VETH, scenario.server_to_client),
    ]
    for namespace, dev, setting in commands:
        if not setting:
            continue
        command = [
            *sudo_prefix(args),
            "ip",
            "netns",
            "exec",
            namespace,
            "tc",
            "qdisc",
            "replace",
            "dev",
            dev,
            "root",
            "netem",
            *shlex.split(setting),
        ]
        run_checked(command, scenario_dir / "netem" / f"{namespace}-{dev}")


def delete_qdiscs(args: argparse.Namespace, log_dir: Path) -> None:
    for namespace, dev in [(CLIENT_NS, CLIENT_VETH), (SERVER_NS, SERVER_VETH)]:
        command = [
            *sudo_prefix(args),
            "ip",
            "netns",
            "exec",
            namespace,
            "tc",
            "qdisc",
            "del",
            "dev",
            dev,
            "root",
        ]
        run_checked(
            command,
            log_dir / "qdisc-cleanup" / f"{namespace}-{dev}",
            check=False,
            ignored_stderr_texts=["Cannot delete qdisc with handle of zero"],
        )


def load_latest_comparison(compare_root: Path) -> dict[str, Any]:
    candidates = sorted(compare_root.glob("*/comparison.json"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        return {"_path": None, "results": [], "local_error": "comparison.json not found"}
    path = candidates[-1]
    data = json.loads(path.read_text(encoding="utf-8"))
    data["_path"] = str(path)
    return data


def classify_scenario(
    args: argparse.Namespace,
    scenario: Scenario,
    returncode: int,
    comparison: dict[str, Any],
) -> dict[str, Any]:
    notes: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []
    result = first_comparison_result(comparison)
    upstream = result.get("metrics", {}).get("upstream", {}) if result else {}
    rust = result.get("metrics", {}).get("irtt_rs", {}) if result else {}

    if returncode != 0:
        failures.append(f"comparison harness exited {returncode}")
    if not result:
        failures.append("comparison result missing")
    for label, metrics in [("upstream", upstream), ("irtt-rs", rust)]:
        if metrics.get("rtt_mean_us") is None:
            failures.append(f"{label} mean RTT missing")
        if (metrics.get("received") or 0) <= 0:
            failures.append(f"{label} received no packets")

    mean_delta_us = None
    if upstream.get("rtt_mean_us") is not None and rust.get("rtt_mean_us") is not None:
        mean_delta_us = rust["rtt_mean_us"] - upstream["rtt_mean_us"]
        baseline = scenario.expected_rtt_us or max(abs(upstream["rtt_mean_us"]), 1.0)
        if scenario.mode == "deterministic":
            tolerance = max(args.mean_abs_tolerance_us, args.mean_pct_tolerance * baseline)
            if abs(mean_delta_us) > tolerance:
                failures.append(
                    f"mean RTT delta {mean_delta_us:.1f}us exceeds deterministic tolerance {tolerance:.1f}us"
                )
        elif scenario.mode == "jitter":
            tolerance = max(args.jitter_mean_abs_tolerance_us, 0.10 * baseline)
            if abs(mean_delta_us) > tolerance:
                failures.append(
                    f"mean RTT delta {mean_delta_us:.1f}us exceeds jitter tolerance {tolerance:.1f}us"
                )

    if scenario.mode == "deterministic":
        for label, metrics in [("upstream", upstream), ("irtt-rs", rust)]:
            loss = metrics.get("loss_percent")
            if loss is not None and loss > 0.5:
                failures.append(f"{label} observed unexpected loss {loss:.3f}%")
    elif scenario.mode == "loss":
        upstream_loss = upstream.get("loss_percent")
        rust_loss = rust.get("loss_percent")
        expected_loss = expected_rtt_loss_percent(scenario)
        if upstream_loss is not None and rust_loss is not None:
            loss_delta = rust_loss - upstream_loss
            if abs(loss_delta) > args.loss_rate_warning_pct:
                warnings.append(
                    f"loss-rate delta {loss_delta:.3f}% exceeds warning threshold "
                    f"{args.loss_rate_warning_pct:.3f}%"
                )
        for label, observed_loss in [("upstream", upstream_loss), ("irtt-rs", rust_loss)]:
            if observed_loss is None:
                failures.append(f"{label} loss rate missing")
            elif observed_loss <= 0.0:
                failures.append(f"{label} observed no loss in loss scenario")
            elif abs(observed_loss - expected_loss) > args.loss_abs_tolerance_pct:
                failures.append(
                    f"{label} loss {observed_loss:.3f}% outside expected "
                    f"{expected_loss:.3f}% +/- {args.loss_abs_tolerance_pct:.3f}%"
                )

    comparison_notes = result.get("comparison", {}).get("notes", []) if result else []
    notes.extend(comparison_notes)
    classification = "fail" if failures else ("warning" if warnings else "pass")
    return {
        "classification": classification,
        "failures": failures,
        "warnings": warnings,
        "notes": sorted(set(notes)),
        "mean_rtt_delta_us": mean_delta_us,
        "expected_loss_percent": expected_rtt_loss_percent(scenario) if scenario.mode == "loss" else None,
        "upstream": upstream,
        "irtt_rs": rust,
    }


def expected_rtt_loss_percent(scenario: Scenario) -> float:
    client_to_server_loss = netem_loss_percent(scenario.client_to_server)
    server_to_client_loss = netem_loss_percent(scenario.server_to_client)
    success_probability = (1.0 - client_to_server_loss / 100.0) * (1.0 - server_to_client_loss / 100.0)
    return 100.0 * (1.0 - success_probability)


def netem_loss_percent(setting: str) -> float:
    match = re.search(r"(?:^|\s)loss\s+([0-9]+(?:\.[0-9]+)?)%", setting)
    if match is None:
        return 0.0
    return float(match.group(1))


def first_comparison_result(comparison: dict[str, Any]) -> dict[str, Any] | None:
    results = comparison.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            return first
    return None


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_scenario_markdown(path: Path, result: dict[str, Any]) -> None:
    lines = [
        f"# {result['scenario']['name']}",
        "",
        f"classification: `{result['classification']}`",
        f"client-to-server netem: `{result['qdisc']['client_to_server']}`",
        f"server-to-client netem: `{result['qdisc']['server_to_client']}`",
        f"comparison_json: `{result.get('comparison_json')}`",
        f"mean_rtt_delta_us: `{format_optional(result.get('mean_rtt_delta_us'))}`",
        f"expected_loss_percent: `{format_optional(result.get('expected_loss_percent'))}`",
        "",
        "| client | sent | received | loss % | mean RTT us |",
        "| --- | ---: | ---: | ---: | ---: |",
        markdown_metric_row("upstream", result["upstream"]),
        markdown_metric_row("irtt-rs", result["irtt_rs"]),
    ]
    if result["failures"]:
        lines.extend(["", "## Failures"])
        lines.extend(f"- {item}" for item in result["failures"])
    if result["warnings"]:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {item}" for item in result["warnings"])
    if result["notes"]:
        lines.extend(["", "## Comparison Notes"])
        lines.extend(f"- {item}" for item in result["notes"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def markdown_metric_row(label: str, metrics: dict[str, Any]) -> str:
    return (
        f"| {label} | {format_optional(metrics.get('sent'))} | "
        f"{format_optional(metrics.get('received'))} | "
        f"{format_optional(metrics.get('loss_percent'))} | "
        f"{format_optional(metrics.get('rtt_mean_us'))} |"
    )


def format_optional(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def write_run_summary(run_dir: Path, results: list[dict[str, Any]]) -> None:
    write_json(run_dir / "netem-summary.json", {"results": results})
    lines = [
        "# Netem IRTT Interop Run",
        "",
        "| scenario | classification | expected RTT us | upstream median RTT us | irtt-rs median RTT us | upstream loss % | irtt-rs loss % | mean RTT delta us |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        scenario = result["scenario"]
        upstream = result["upstream"]
        rust = result["irtt_rs"]
        lines.append(
            f"| [{scenario['name']}]({scenario['name']}/summary.md) | "
            f"{result['classification']} | "
            f"{format_optional(scenario.get('expected_rtt_us'))} | "
            f"{format_optional(upstream.get('rtt_median_us'))} | "
            f"{format_optional(rust.get('rtt_median_us'))} | "
            f"{format_optional(upstream.get('loss_percent'))} | "
            f"{format_optional(rust.get('loss_percent'))} | "
            f"{format_optional(result.get('mean_rtt_delta_us'))} |"
        )
    failures = [(result["scenario"]["name"], item) for result in results for item in result["failures"]]
    warnings = [(result["scenario"]["name"], item) for result in results for item in result["warnings"]]
    if failures:
        lines.extend(["", "## Failures"])
        lines.extend(f"- [{scenario}]({scenario}/summary.md): {item}" for scenario, item in failures)
    if warnings:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- [{scenario}]({scenario}/summary.md): {item}" for scenario, item in warnings)
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def stop_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)


def cleanup(args: argparse.Namespace, quiet: bool) -> None:
    if shutil.which("ip") is None:
        return
    if args.sudo and shutil.which(args.sudo) is None:
        return
    commands = [
        [*sudo_prefix(args), "ip", "netns", "exec", CLIENT_NS, "tc", "qdisc", "del", "dev", CLIENT_VETH, "root"],
        [*sudo_prefix(args), "ip", "netns", "exec", SERVER_NS, "tc", "qdisc", "del", "dev", SERVER_VETH, "root"],
        [*sudo_prefix(args), "ip", "netns", "delete", CLIENT_NS],
        [*sudo_prefix(args), "ip", "netns", "delete", SERVER_NS],
    ]
    for command in commands:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not quiet and completed.returncode != 0:
            print(f"cleanup command failed: {shlex.join(command)}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
