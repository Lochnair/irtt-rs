#!/usr/bin/env python3
"""Run paired upstream irtt and irtt-rs client comparisons.

This is a black-box interoperability harness. It never reads upstream source; it
only invokes the upstream `irtt` executable and compares its captured output with
the local Rust client output.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median
from typing import Any


DEFAULT_REMOTE_TARGETS = [
    ("netperf-eu.bufferbloat.net", "15s", "250ms", "remote-eu"),
    ("45.128.222.138", "15s", "250ms", "remote-singapore"),
]
DEFAULT_LOCAL_TARGET = ("127.0.0.1", "10s", "100ms", "local")
DEFAULT_OUTPUT_ROOT = Path("target/interop-comparisons")
DEFAULT_IRTT_RS_COMMAND = "target/debug/irtt-cli"


@dataclass
class CommandResult:
    name: str
    command: list[str]
    started_at: str
    finished_at: str
    elapsed_seconds: float
    returncode: int
    stdout_path: str
    stderr_path: str
    extra_paths: dict[str, str] = field(default_factory=dict)


@dataclass
class PairResult:
    upstream: CommandResult
    rust: CommandResult
    start_skew_ms: float


@dataclass
class Metrics:
    source: str
    sent: int | None = None
    received: int | None = None
    lost: int | None = None
    loss_percent: float | None = None
    rtt_min_us: float | None = None
    rtt_mean_us: float | None = None
    rtt_median_us: float | None = None
    rtt_max_us: float | None = None
    raw_rtt_min_us: float | None = None
    raw_rtt_mean_us: float | None = None
    raw_rtt_median_us: float | None = None
    raw_rtt_max_us: float | None = None
    adjusted_rtt_min_us: float | None = None
    adjusted_rtt_mean_us: float | None = None
    adjusted_rtt_median_us: float | None = None
    adjusted_rtt_max_us: float | None = None
    samples_path: str | None = None
    first_send_wall_ns: int | None = None
    parse_notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in self.__dict__.items()
            if value is not None and value != []
        }


@dataclass
class TestCase:
    name: str
    target: str
    duration: str
    interval: str
    is_local: bool = False


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "target"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare upstream irtt and irtt-rs clients with concurrent runs."
    )
    parser.add_argument(
        "--target",
        action="append",
        help=(
            "Target host or host:port. Repeat to run multiple custom targets. "
            "When omitted, the default remote targets plus localhost are used."
        ),
    )
    parser.add_argument(
        "--duration",
        help="Override all test durations, for example 10s or 1500ms.",
    )
    parser.add_argument(
        "--interval",
        help="Override all probe intervals, for example 250ms.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Directory for run artifacts (default: {DEFAULT_OUTPUT_ROOT}).",
    )
    parser.add_argument(
        "--remote-only",
        action="store_true",
        help="Run only remote/default or explicitly supplied non-local targets.",
    )
    parser.add_argument(
        "--include-local",
        action="store_true",
        help="Also run the localhost test when custom --target values are supplied.",
    )
    parser.add_argument(
        "--upstream-irtt",
        default="irtt",
        help="Path to the upstream irtt executable (default: irtt).",
    )
    parser.add_argument(
        "--irtt-rs-command",
        default=DEFAULT_IRTT_RS_COMMAND,
        help=(
            "Command prefix or direct path for the Rust client. Arguments for "
            "target, duration, interval, and machine output are appended. "
            f"Default: {DEFAULT_IRTT_RS_COMMAND!r}"
        ),
    )
    parser.add_argument(
        "--localhost-port",
        type=int,
        default=2112,
        help="Port used by the localhost upstream irtt server check (default: 2112).",
    )
    parser.add_argument(
        "--upstream-extra-arg",
        action="append",
        default=[],
        help="Extra argument passed to upstream `irtt client`; repeat as needed.",
    )
    parser.add_argument(
        "--rust-extra-arg",
        action="append",
        default=[],
        help="Extra argument passed to irtt-rs; repeat as needed.",
    )
    return parser.parse_args()


def build_cases(args: argparse.Namespace) -> list[TestCase]:
    cases: list[TestCase] = []
    if args.target:
        for target in args.target:
            target_host, target_port = split_host_port(target)
            is_local = target_host in {"127.0.0.1", "localhost", "::1"}
            if args.remote_only and is_local:
                continue
            cases.append(
                TestCase(
                    name=safe_name(target),
                    target=target,
                    duration=args.duration or "15s",
                    interval=args.interval or "250ms",
                    is_local=is_local,
                )
            )
        if args.include_local and not args.remote_only:
            host, duration, interval, name = DEFAULT_LOCAL_TARGET
            cases.append(
                TestCase(
                    name=name,
                    target=f"{host}:{args.localhost_port}",
                    duration=args.duration or duration,
                    interval=args.interval or interval,
                    is_local=True,
                )
            )
    else:
        for host, duration, interval, name in DEFAULT_REMOTE_TARGETS:
            cases.append(
                TestCase(
                    name=name,
                    target=host,
                    duration=args.duration or duration,
                    interval=args.interval or interval,
                )
            )
        if not args.remote_only:
            host, duration, interval, name = DEFAULT_LOCAL_TARGET
            cases.append(
                TestCase(
                    name=name,
                    target=f"{host}:{args.localhost_port}",
                    duration=args.duration or duration,
                    interval=args.interval or interval,
                    is_local=True,
                )
            )
    return cases


def split_host_port(target: str) -> tuple[str, int | None]:
    if target.startswith("["):
        match = re.match(r"^\[([^\]]+)\](?::(\d+))?$", target)
        if match:
            port = int(match.group(2)) if match.group(2) else None
            return match.group(1), port
        return target, None
    if target.count(":") == 1:
        host, port = target.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return target, None


def local_instruction(port: int, upstream: str) -> str:
    return (
        f"localhost:{port} is not reachable by upstream irtt no-test. "
        "Start a local upstream server manually, for example:\n\n"
        f"  {shlex.quote(upstream)} server -b 127.0.0.1:{port} --tstamp=dual\n\n"
        "Then rerun this harness, or pass --remote-only to intentionally skip "
        "the local test."
    )


def classify_local_preflight_failure(
    exc: BaseException | None, stdout: str = "", stderr: str = ""
) -> str:
    text = f"{exc or ''}\n{stdout}\n{stderr}".lower()
    if isinstance(exc, subprocess.TimeoutExpired) or any(
        marker in text for marker in ["timed out", "timeout", "i/o timeout", "deadline exceeded"]
    ):
        return "timeout: no response/firewall"
    if any(
        marker in text
        for marker in [
            "connection refused",
            "connection reset by peer",
            "connect: refused",
            "actively refused",
        ]
    ):
        return "connection refused: likely no local server"
    if isinstance(exc, PermissionError) or any(
        marker in text
        for marker in [
            "permission denied",
            "operation not permitted",
            "address already in use",
            "bind: permission",
            "bind: address",
        ]
    ):
        return "permission/bind error: local environment permission issue"
    return "other: unknown local preflight failure"


def check_local_server(args: argparse.Namespace) -> tuple[bool, str]:
    target = f"127.0.0.1:{args.localhost_port}"
    command = [
        args.upstream_irtt,
        "client",
        "-n",
        "-Q",
        "--timeouts=1s",
        target,
    ]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        classification = classify_local_preflight_failure(exc)
        return (
            False,
            f"{local_instruction(args.localhost_port, args.upstream_irtt)}\n"
            f"check classification: {classification}\n"
            f"check error: {exc}",
        )
    if completed.returncode == 0:
        return True, ""
    details = (completed.stderr or completed.stdout or "").strip()
    classification = classify_local_preflight_failure(
        None, completed.stdout or "", completed.stderr or ""
    )
    suffix = f"\ncheck output:\n{details}" if details else ""
    return (
        False,
        local_instruction(args.localhost_port, args.upstream_irtt)
        + f"\ncheck classification: {classification}"
        + suffix,
    )


def preflight_build_irtt_cli(run_dir: Path) -> bool:
    command = ["cargo", "build", "-p", "irtt-cli"]
    started = utc_now()
    print(f"[{started}] preflight build: {shlex.join(command)}", flush=True)
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    (run_dir / "preflight-build.stdout").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "preflight-build.stderr").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        print(
            f"preflight build failed with exit {completed.returncode}; "
            f"see {run_dir / 'preflight-build.stderr'}",
            file=sys.stderr,
        )
        return False
    return True


def run_case(
    case: TestCase, run_dir: Path, args: argparse.Namespace
) -> tuple[dict[str, Any], bool]:
    case_dir = run_dir / case.name
    case_dir.mkdir(parents=True, exist_ok=True)

    upstream_json = case_dir / "upstream.json"
    upstream_cmd = [
        args.upstream_irtt,
        "client",
        "-d",
        case.duration,
        "-i",
        case.interval,
        "-q",
        "-o",
        str(upstream_json),
        *args.upstream_extra_arg,
        case.target,
    ]
    rust_cmd = [
        *shlex.split(args.irtt_rs_command),
        case.target,
        "--duration",
        case.duration,
        "--interval",
        case.interval,
        "--output",
        "machine",
        *args.rust_extra_arg,
    ]

    print(
        f"[{utc_now()}] running {case.name}: target={case.target} "
        f"duration={case.duration} interval={case.interval}",
        flush=True,
    )
    pair_result = run_pair(case_dir, upstream_cmd, rust_cmd)
    upstream_result = pair_result.upstream
    rust_result = pair_result.rust

    rust_samples = case_dir / "irtt-rs-rtt-samples.csv"
    upstream_samples = case_dir / "upstream-rtt-samples.csv"
    rust_metrics = parse_rust_machine(case_dir / "irtt-rs.stdout", rust_samples)
    upstream_metrics = parse_upstream(case_dir / "upstream.stdout", upstream_json, upstream_samples)

    comparison = classify_difference(upstream_metrics, rust_metrics)
    first_send_skew_ms = paired_first_send_skew_ms(upstream_metrics, rust_metrics)
    result = {
        "case": case.__dict__,
        "upstream": upstream_result.__dict__,
        "irtt_rs": rust_result.__dict__,
        "paired_start_skew_ms": pair_result.start_skew_ms,
        "paired_first_send_skew_ms": first_send_skew_ms,
        "metrics": {
            "upstream": upstream_metrics.as_dict(),
            "irtt_rs": rust_metrics.as_dict(),
        },
        "comparison": comparison,
    }

    write_json(case_dir / "comparison.json", result)
    write_case_summary(case_dir / "summary.md", result)
    ok = upstream_result.returncode == 0 and rust_result.returncode == 0
    return result, ok


def paired_first_send_skew_ms(upstream: Metrics, rust: Metrics) -> float | None:
    if upstream.first_send_wall_ns is None or rust.first_send_wall_ns is None:
        return None
    return (rust.first_send_wall_ns - upstream.first_send_wall_ns) / 1_000_000.0


def run_pair(
    case_dir: Path, upstream_cmd: list[str], rust_cmd: list[str]
) -> PairResult:
    (case_dir / "commands.txt").write_text(
        "upstream: " + shlex.join(upstream_cmd) + "\n"
        "irtt-rs:  " + shlex.join(rust_cmd) + "\n",
        encoding="utf-8",
    )
    upstream_stdout = case_dir / "upstream.stdout"
    upstream_stderr = case_dir / "upstream.stderr"
    rust_stdout = case_dir / "irtt-rs.stdout"
    rust_stderr = case_dir / "irtt-rs.stderr"

    upstream_out = upstream_stdout.open("wb")
    upstream_err = upstream_stderr.open("wb")
    upstream_started = utc_now()
    upstream_start_mono = time.monotonic()
    try:
        upstream_proc = subprocess.Popen(
            upstream_cmd,
            stdout=upstream_out,
            stderr=upstream_err,
        )
    except OSError as exc:
        upstream_out.close()
        upstream_err.close()
        upstream_stderr.write_text(f"failed to start upstream irtt: {exc}\n", encoding="utf-8")
        upstream_proc = None

    rust_out = rust_stdout.open("wb")
    rust_err = rust_stderr.open("wb")
    rust_started = utc_now()
    rust_start_mono = time.monotonic()
    try:
        rust_proc = subprocess.Popen(
            rust_cmd,
            stdout=rust_out,
            stderr=rust_err,
        )
    except OSError as exc:
        rust_out.close()
        rust_err.close()
        rust_stderr.write_text(f"failed to start irtt-rs: {exc}\n", encoding="utf-8")
        rust_proc = None

    upstream_rc = wait_process(upstream_proc)
    upstream_finished = utc_now()
    upstream_elapsed = time.monotonic() - upstream_start_mono
    rust_rc = wait_process(rust_proc)
    rust_finished = utc_now()
    rust_elapsed = time.monotonic() - rust_start_mono
    for handle in [upstream_out, upstream_err, rust_out, rust_err]:
        try:
            handle.close()
        except Exception:
            pass

    return PairResult(
        upstream=CommandResult(
            name="upstream",
            command=upstream_cmd,
            started_at=upstream_started,
            finished_at=upstream_finished,
            elapsed_seconds=upstream_elapsed,
            returncode=upstream_rc,
            stdout_path=str(upstream_stdout),
            stderr_path=str(upstream_stderr),
            extra_paths={"json": str(case_dir / "upstream.json")},
        ),
        rust=CommandResult(
            name="irtt-rs",
            command=rust_cmd,
            started_at=rust_started,
            finished_at=rust_finished,
            elapsed_seconds=rust_elapsed,
            returncode=rust_rc,
            stdout_path=str(rust_stdout),
            stderr_path=str(rust_stderr),
        ),
        start_skew_ms=(rust_start_mono - upstream_start_mono) * 1000.0,
    )


def wait_process(proc: subprocess.Popen[bytes] | None) -> int:
    if proc is None:
        return 127
    try:
        return proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            return proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait()


def parse_kv_line(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in shlex.split(line):
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    return fields


def parse_rust_machine(path: Path, samples_path: Path) -> Metrics:
    metrics = Metrics(source="irtt-rs", samples_path=str(samples_path))
    effective: list[float] = []
    raw: list[float] = []
    adjusted: list[float] = []
    sent_seqs: set[int] = set()
    received_seqs: set[int] = set()
    lost = 0
    rows = ["seq,effective_rtt_us,raw_rtt_us,adjusted_rtt_us,server_processing_us\n"]

    if not path.exists():
        metrics.parse_notes.append(f"missing {path}")
        return metrics

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        fields = parse_kv_line(line)
        event = fields.get("event")
        seq = parse_int(fields.get("seq"))
        if event == "echo_reply":
            if seq is not None:
                received_seqs.add(seq)
                sent_seqs.add(seq)
            effective_value = parse_float(fields.get("effective_rtt_us"))
            raw_value = parse_float(fields.get("raw_rtt_us"))
            adjusted_value = parse_float(fields.get("adjusted_rtt_us"))
            server_processing = parse_float(fields.get("server_processing_us"))
            append_if_number(effective, effective_value)
            append_if_number(raw, raw_value)
            append_if_number(adjusted, adjusted_value)
            if metrics.first_send_wall_ns is None:
                metrics.first_send_wall_ns = parse_int(fields.get("client_send_wall_ns"))
            rows.append(
                f"{blank_none(seq)},{blank_none(effective_value)},{blank_none(raw_value)},"
                f"{blank_none(adjusted_value)},{blank_none(server_processing)}\n"
            )
        elif event == "loss":
            lost += 1
            if seq is not None:
                sent_seqs.add(seq)
        elif event in {"duplicate", "late"}:
            metrics.parse_notes.append(f"excluded {event} event from primary RTT stats")

    samples_path.write_text("".join(rows), encoding="utf-8")
    metrics.sent = len(sent_seqs) if sent_seqs else None
    metrics.received = len(received_seqs)
    metrics.lost = lost if lost else (
        metrics.sent - metrics.received if metrics.sent is not None else None
    )
    if metrics.sent:
        metrics.loss_percent = 100.0 * (metrics.lost or 0) / metrics.sent
    fill_stats(metrics, effective, "rtt")
    fill_stats(metrics, raw, "raw_rtt")
    fill_stats(metrics, adjusted, "adjusted_rtt")
    if not effective:
        metrics.parse_notes.append("no echo_reply effective_rtt_us samples parsed")
    return metrics


def parse_upstream(stdout_path: Path, json_path: Path, samples_path: Path) -> Metrics:
    metrics = Metrics(source="upstream", samples_path=str(samples_path))
    samples: list[float] = []
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8", errors="replace"))
            extract_upstream_json_metrics(data, metrics, samples)
        except json.JSONDecodeError as exc:
            metrics.parse_notes.append(f"failed to parse upstream JSON: {exc}")
    else:
        metrics.parse_notes.append(f"missing upstream JSON output: {json_path}")

    if stdout_path.exists():
        parse_upstream_text(stdout_path.read_text(encoding="utf-8", errors="replace"), metrics)

    samples_path.write_text(
        "rtt_us\n" + "".join(f"{value}\n" for value in samples),
        encoding="utf-8",
    )
    if samples:
        fill_stats(metrics, samples, "rtt")
    if not samples and all(
        value is None
        for value in [
            metrics.rtt_min_us,
            metrics.rtt_mean_us,
            metrics.rtt_median_us,
            metrics.rtt_max_us,
        ]
    ):
        metrics.parse_notes.append(
            "upstream RTT samples/stats were not recognized; inspect upstream.json manually"
        )
    return metrics


def extract_upstream_json_metrics(data: Any, metrics: Metrics, samples: list[float]) -> None:
    metrics.first_send_wall_ns = extract_upstream_first_send_wall_ns(data)
    for key, value in walk_items(data):
        key_norm = leaf_key(key)
        if metrics.sent is None and key_norm in {
            "packetssent",
            "packetssend",
            "sent",
            "send",
            "npackets",
        }:
            metrics.sent = parse_int(value)
        elif metrics.received is None and key_norm in {
            "packetsreceived",
            "packetsrecv",
            "received",
            "recv",
        }:
            metrics.received = parse_int(value)
        elif metrics.lost is None and key_norm in {"packetslost", "lost"}:
            metrics.lost = parse_int(value)
        elif metrics.loss_percent is None and key_norm in {
            "packetlosspercent",
            "losspercent",
            "losspct",
        }:
            metrics.loss_percent = parse_float(value)

    for key, value in walk_items(data):
        if is_upstream_rtt_stats_key(key) and isinstance(value, dict):
            apply_stat_dict(metrics, value, "rtt")

    collect_upstream_samples(data, samples)


def walk_items(value: Any, parent_key: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            joined = f"{parent_key}.{key}" if parent_key else str(key)
            yield joined, child
            yield from walk_items(child, joined)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_items(child, f"{parent_key}[{index}]")


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def leaf_key(value: str) -> str:
    leaf = re.split(r"[.\[]", value)[-1].rstrip("]")
    return normalize_key(leaf)


def is_upstream_rtt_stats_key(key: str) -> bool:
    parts = [normalize_key(part) for part in key.split(".")]
    if not parts:
        return False
    leaf = parts[-1]
    if leaf not in {"rtt", "roundtrip", "roundtriptime"}:
        return False
    if len(parts) == 1:
        return True
    return parts[-2] in {"stats", "statistics", "summary"}


def apply_stat_dict(metrics: Metrics, value: dict[str, Any], prefix: str) -> None:
    by_key = {normalize_key(str(key)): child for key, child in value.items()}
    for attr, candidates in [
        (f"{prefix}_min_us", ["min", "minimum"]),
        (f"{prefix}_mean_us", ["mean", "avg", "average"]),
        (f"{prefix}_median_us", ["median", "p50"]),
        (f"{prefix}_max_us", ["max", "maximum"]),
    ]:
        if getattr(metrics, attr) is not None:
            continue
        for candidate in candidates:
            if candidate in by_key:
                parsed = parse_duration_to_us(by_key[candidate])
                if parsed is not None:
                    setattr(metrics, attr, parsed)
                    break


def collect_upstream_samples(value: Any, samples: list[float]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if normalize_key(str(key)) == "roundtrips" and isinstance(child, list):
                collect_upstream_round_trip_samples(child, samples)
            else:
                collect_upstream_samples(child, samples)
    elif isinstance(value, list):
        for child in value:
            collect_upstream_samples(child, samples)


def collect_upstream_round_trip_samples(round_trips: list[Any], samples: list[float]) -> None:
    for item in round_trips:
        if not isinstance(item, dict):
            continue
        if str(item.get("lost", "")).lower() == "true":
            continue
        delay = item.get("delay")
        if not isinstance(delay, dict):
            continue
        parsed = parse_duration_to_us(delay.get("rtt"))
        if parsed is not None:
            samples.append(parsed)


def extract_upstream_first_send_wall_ns(data: Any) -> int | None:
    round_trips = data.get("round_trips") if isinstance(data, dict) else None
    if not isinstance(round_trips, list):
        return None
    for item in round_trips:
        if not isinstance(item, dict):
            continue
        if str(item.get("lost", "")).lower() == "true":
            continue
        timestamps = item.get("timestamps")
        if not isinstance(timestamps, dict):
            continue
        client = timestamps.get("client")
        if not isinstance(client, dict):
            continue
        send = client.get("send")
        if not isinstance(send, dict):
            continue
        wall = parse_int(send.get("wall"))
        if wall is not None:
            return wall
    return None


def parse_upstream_text(text: str, metrics: Metrics) -> None:
    packet_match = re.search(
        r"(?P<sent>\d+)\s+packets?\s+sent.*?(?P<received>\d+)\s+"
        r"(?:packets?\s+)?received.*?(?P<loss>[0-9.]+)%\s+"
        r"(?:packet\s+)?loss",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if packet_match:
        metrics.sent = metrics.sent if metrics.sent is not None else int(packet_match.group("sent"))
        metrics.received = (
            metrics.received
            if metrics.received is not None
            else int(packet_match.group("received"))
        )
        metrics.loss_percent = (
            metrics.loss_percent
            if metrics.loss_percent is not None
            else float(packet_match.group("loss"))
        )

    rtt_match = re.search(
        r"(?:rtt|round[- ]trip).*?min/mean/(?:median/)?max.*?=\s*"
        r"(?P<min>[0-9.]+)/(?P<mean>[0-9.]+)/(?:(?P<median>[0-9.]+)/)?(?P<max>[0-9.]+)\s*(?P<unit>us|µs|ms|s)?",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if rtt_match:
        unit = rtt_match.group("unit") or "ms"
        for name in ["min", "mean", "median", "max"]:
            value = rtt_match.group(name)
            if value is None:
                continue
            attr = f"rtt_{name}_us"
            if getattr(metrics, attr) is None:
                setattr(metrics, attr, unit_to_us(float(value), unit))


def run_parser_self_check() -> None:
    metrics = Metrics(source="upstream-self-check")
    samples: list[float] = []
    fixture = {
        "stats": {
            "rtt": {
                "min": "1.2ms",
                "mean": "1.3ms",
                "median": "1.3ms",
                "max": "1.4ms",
            },
            "ipdv_round_trip": {
                "min": "-900us",
                "mean": "-100us",
                "median": "-50us",
                "max": "200us",
            },
        },
        "round_trips": [
            {
                "timestamps": {"client": {"send": {"wall": 111}}},
                "delay": {"rtt": "1.2ms"},
                "ipdv": {"rtt": "-900us"},
            },
            {
                "timestamps": {"client": {"send": {"wall": 222}}},
                "delay": {"rtt": "1.4ms"},
                "ipdv": {"rtt": "-100us"},
            },
            {
                "lost": True,
                "delay": {"rtt": "99ms"},
                "ipdv": {"rtt": "-99ms"},
            },
        ],
    }
    extract_upstream_json_metrics(fixture, metrics, samples)
    expected_samples = [1200.0, 1400.0]
    expected_stats = {
        "rtt_min_us": 1200.0,
        "rtt_mean_us": 1300.0,
        "rtt_median_us": 1300.0,
        "rtt_max_us": 1400.0,
    }
    if samples != expected_samples:
        raise RuntimeError(
            "upstream parser self-check failed: "
            f"expected RTT samples {expected_samples}, got {samples}"
        )
    for attr, expected in expected_stats.items():
        actual = getattr(metrics, attr)
        if actual != expected:
            raise RuntimeError(
                "upstream parser self-check failed: "
                f"expected {attr}={expected}, got {actual}"
            )
    if metrics.first_send_wall_ns != 111:
        raise RuntimeError(
            "upstream parser self-check failed: "
            f"expected first_send_wall_ns=111, got {metrics.first_send_wall_ns}"
        )


def classify_difference(upstream: Metrics, rust: Metrics) -> dict[str, Any]:
    notes: list[str] = []
    deltas: dict[str, Any] = {}

    if upstream.sent is not None and rust.sent is not None:
        deltas["sent"] = rust.sent - upstream.sent
        if abs(deltas["sent"]) > 1:
            notes.append("packet count/configuration difference")
    if upstream.received is not None and rust.received is not None:
        deltas["received"] = rust.received - upstream.received
        if abs(deltas["received"]) > 1:
            notes.append("received packet/loss difference")
    if upstream.loss_percent is not None and rust.loss_percent is not None:
        deltas["loss_percent"] = rust.loss_percent - upstream.loss_percent
        if abs(deltas["loss_percent"]) >= 5.0:
            notes.append("packet loss difference")
    if upstream.rtt_mean_us is not None and rust.rtt_mean_us is not None:
        deltas["mean_rtt_us"] = rust.rtt_mean_us - upstream.rtt_mean_us
        baseline = max(abs(upstream.rtt_mean_us), 1.0)
        if abs(deltas["mean_rtt_us"]) / baseline > 0.25 and abs(deltas["mean_rtt_us"]) > 1000:
            notes.append(
                "RTT difference after parser self-check; potential network/timing difference"
            )

    if upstream.parse_notes or rust.parse_notes:
        notes.append("output parsing limitation")
    if not notes:
        notes.append("no obvious difference detected by conservative parser")

    return {"deltas": deltas, "notes": sorted(set(notes))}


def fill_stats(metrics: Metrics, values: list[float], prefix: str) -> None:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return
    setattr(metrics, f"{prefix}_min_us", min(clean))
    setattr(metrics, f"{prefix}_mean_us", mean(clean))
    setattr(metrics, f"{prefix}_median_us", median(clean))
    setattr(metrics, f"{prefix}_max_us", max(clean))


def parse_duration_to_us(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        # Upstream Go JSON commonly serializes time.Duration as nanoseconds.
        return float(value) / 1000.0
    if isinstance(value, str):
        stripped = value.strip()
        numeric = parse_float(stripped)
        if numeric is not None:
            return numeric / 1000.0
        match = re.fullmatch(r"(-?[0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|s)", stripped)
        if match:
            return unit_to_us(float(match.group(1)), match.group(2))
    if isinstance(value, dict):
        for key in ["us", "usec", "microseconds"]:
            if key in value:
                return parse_float(value[key])
        for key in ["ns", "nsec", "nanoseconds"]:
            parsed = parse_float(value[key]) if key in value else None
            if parsed is not None:
                return parsed / 1000.0
    return None


def unit_to_us(value: float, unit: str) -> float:
    if unit in {"us", "µs"}:
        return value
    if unit == "ns":
        return value / 1000.0
    if unit == "ms":
        return value * 1000.0
    if unit == "s":
        return value * 1_000_000.0
    return value


def parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def append_if_number(values: list[float], value: float | None) -> None:
    if value is not None and math.isfinite(value):
        values.append(value)


def blank_none(value: Any) -> str:
    return "" if value is None else str(value)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_case_summary(path: Path, result: dict[str, Any]) -> None:
    upstream = result["metrics"]["upstream"]
    rust = result["metrics"]["irtt_rs"]
    lines = [
        f"# {result['case']['name']}",
        "",
        f"target: `{result['case']['target']}`",
        f"duration: `{result['case']['duration']}`",
        f"interval: `{result['case']['interval']}`",
        f"paired_start_skew_ms: `{result['paired_start_skew_ms']:.3f}`",
        "paired_first_send_skew_ms: `"
        + format_optional_float(result.get("paired_first_send_skew_ms"))
        + "`",
        "",
        "| client | exit | sent | received | loss % | RTT min/mean/median/max us |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
        summary_row("upstream", result["upstream"]["returncode"], upstream),
        summary_row("irtt-rs", result["irtt_rs"]["returncode"], rust),
        "",
        "## Difference Notes",
    ]
    for note in result["comparison"]["notes"]:
        lines.append(f"- {note}")
    lines.extend(["", "## irtt-rs RTT Semantics", ""])
    lines.append("| field | min/mean/median/max us |")
    lines.append("| --- | --- |")
    lines.append(
        "| effective | "
        + metric_quad(rust, ["rtt_min_us", "rtt_mean_us", "rtt_median_us", "rtt_max_us"])
        + " |"
    )
    lines.append(
        "| raw | "
        + metric_quad(
            rust,
            [
                "raw_rtt_min_us",
                "raw_rtt_mean_us",
                "raw_rtt_median_us",
                "raw_rtt_max_us",
            ],
        )
        + " |"
    )
    lines.append(
        "| adjusted | "
        + metric_quad(
            rust,
            [
                "adjusted_rtt_min_us",
                "adjusted_rtt_mean_us",
                "adjusted_rtt_median_us",
                "adjusted_rtt_max_us",
            ],
        )
        + " |"
    )
    lines.extend(["", "## Artifacts"])
    for label, command in [("upstream", result["upstream"]), ("irtt-rs", result["irtt_rs"])]:
        lines.append(f"- {label} stdout: `{command['stdout_path']}`")
        lines.append(f"- {label} stderr: `{command['stderr_path']}`")
    if upstream.get("samples_path"):
        lines.append(f"- upstream samples: `{upstream['samples_path']}`")
    if rust.get("samples_path"):
        lines.append(f"- irtt-rs samples: `{rust['samples_path']}`")
    for side, metrics in [("upstream", upstream), ("irtt-rs", rust)]:
        if metrics.get("parse_notes"):
            lines.extend(["", f"## {side} Parse Notes"])
            for note in metrics["parse_notes"]:
                lines.append(f"- {note}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summary_row(name: str, exit_code: int, metrics: dict[str, Any]) -> str:
    rtt = metric_quad(metrics, ["rtt_min_us", "rtt_mean_us", "rtt_median_us", "rtt_max_us"])
    return (
        f"| {name} | {exit_code} | {format_metric(metrics.get('sent'))} | "
        f"{format_metric(metrics.get('received'))} | "
        f"{format_metric(metrics.get('loss_percent'))} | {rtt} |"
    )


def metric_quad(metrics: dict[str, Any], keys: list[str]) -> str:
    return "/".join(format_metric(metrics.get(key)) for key in keys)


def format_metric(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def format_optional_float(value: Any) -> str:
    if value is None:
        return "unavailable"
    return f"{float(value):.3f}"


def write_index(run_dir: Path, results: list[dict[str, Any]], local_error: str | None) -> None:
    lines = [
        "# IRTT Client Comparison Run",
        "",
        f"created_at: `{utc_now()}`",
        "",
        "| case | target | upstream exit | irtt-rs exit | notes |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for result in results:
        notes = "; ".join(result["comparison"]["notes"])
        lines.append(
            f"| [{result['case']['name']}]({result['case']['name']}/summary.md) | "
            f"`{result['case']['target']}` | {result['upstream']['returncode']} | "
            f"{result['irtt_rs']['returncode']} | {notes} |"
        )
    if local_error:
        lines.extend(["", "## Local Server Required", "", local_error])
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_json(run_dir / "comparison.json", {"results": results, "local_error": local_error})


def main() -> int:
    args = parse_args()
    try:
        run_parser_self_check()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    run_stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(args.output_dir) / run_stamp
    run_dir.mkdir(parents=True, exist_ok=True)
    cases = build_cases(args)
    if not cases:
        print("no test cases selected", file=sys.stderr)
        return 2

    local_error: str | None = None
    if any(case.is_local for case in cases):
        ok, message = check_local_server(args)
        if not ok:
            local_error = message
            print(message, file=sys.stderr)
            cases = [case for case in cases if not case.is_local]

    if cases and not preflight_build_irtt_cli(run_dir):
        write_index(run_dir, [], local_error)
        return 1

    results: list[dict[str, Any]] = []
    all_ok = local_error is None
    for case in cases:
        result, ok = run_case(case, run_dir, args)
        results.append(result)
        all_ok = all_ok and ok

    write_index(run_dir, results, local_error)
    print(f"[{utc_now()}] wrote comparison artifacts to {run_dir}", flush=True)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
