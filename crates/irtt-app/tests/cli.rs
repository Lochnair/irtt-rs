#[cfg(feature = "client")]
#[path = "support/in_tree_server.rs"]
mod in_tree_server;

#[cfg(feature = "client")]
use irtt_server::ServerConfig;
use std::process::Command;

#[cfg(feature = "client")]
struct ClientProcess(std::process::Child);

#[cfg(feature = "client")]
impl Drop for ClientProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(feature = "client")]
impl ClientProcess {
    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "client did not terminate within five seconds"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

// Dispatch/help/availability belong to the application and run in every build.
#[test]
fn dispatcher_reports_and_runs_the_enabled_applets() {
    let dispatcher = env!("CARGO_BIN_EXE_irtt-rs");
    let help = Command::new(dispatcher).arg("--help").output().unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    for (applet, enabled) in [
        ("client", cfg!(feature = "client")),
        ("server", cfg!(feature = "server")),
        ("tui", cfg!(feature = "tui")),
    ] {
        let row = help
            .lines()
            .find(|line| line.trim_start().starts_with(applet))
            .unwrap();
        assert_eq!(row.contains("not available"), !enabled);
        let output = Command::new(dispatcher)
            .args([applet, "--help"])
            .output()
            .unwrap();
        assert_eq!(output.status.success(), enabled, "{applet}");
        if !enabled {
            assert!(String::from_utf8(output.stderr)
                .unwrap()
                .contains("not available"));
        }
    }
    let binaries = [
        dispatcher,
        #[cfg(feature = "client")]
        env!("CARGO_BIN_EXE_irtt-client"),
        #[cfg(feature = "server")]
        env!("CARGO_BIN_EXE_irtt-server"),
        #[cfg(feature = "tui")]
        env!("CARGO_BIN_EXE_irtt-tui"),
    ];
    for binary in binaries {
        assert!(Command::new(binary)
            .arg("--help")
            .output()
            .unwrap()
            .status
            .success());
        assert!(!Command::new(binary)
            .arg("--invalid-option")
            .output()
            .unwrap()
            .status
            .success());
    }
}

// The default presentation must reach a meaningful packet summary for a finite run.
#[cfg(feature = "client")]
#[test]
fn default_client_output_emits_a_packet_summary() {
    use std::{io::Read, process::Stdio, thread};
    let server = in_tree_server::InTreeServer::start(ServerConfig::default());
    let mut client = ClientProcess(
        Command::new(env!("CARGO_BIN_EXE_irtt-client"))
            .env_clear()
            .args([
                "--duration",
                "100ms",
                "--interval",
                "20ms",
                &server.addr.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stdout = client.0.stdout.take().unwrap();
    let stdout = thread::spawn(move || {
        let mut text = String::new();
        stdout.read_to_string(&mut text).unwrap();
        text
    });
    let mut stderr = client.0.stderr.take().unwrap();
    let stderr = thread::spawn(move || {
        let mut text = String::new();
        stderr.read_to_string(&mut text).unwrap();
        text
    });
    let status = client.wait();
    let stdout = stdout.join().unwrap();
    let stderr = stderr.join().unwrap();
    assert!(status.success(), "{stderr}");
    assert!(stdout.contains("irtt-rs summary"), "{stdout}");
    let packets = stdout
        .lines()
        .find(|line| line.starts_with("packets:"))
        .expect("default output must include the packet summary");
    for field in ["sent=", "received="] {
        let count = packets
            .split_whitespace()
            .find_map(|word| word.strip_prefix(field))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(
            count > 0,
            "summary must report {field} a positive count: {packets}"
        );
    }
}

#[cfg(feature = "client")]
#[test]
fn target_authentication_inherits_overrides_or_disables_the_global_key() {
    use clap::Parser;
    use irtt_app::cmd::client::args::ClientArgs;
    use irtt_client::{managed::TargetAuth, Authentication, HmacKey};
    let args = ClientArgs::try_parse_from([
        "irtt-client",
        "--hmac",
        "CONSPICUOUS_GLOBAL_HMAC_KEY",
        "inherit=127.0.0.1:2112",
        "override=127.0.0.1:2113@hmac=CONSPICUOUS_TARGET_HMAC_KEY",
        "disable=127.0.0.1:2114@hmac=",
    ])
    .unwrap();
    let setup = args.prepare().unwrap();
    assert_eq!(
        setup.managed_config().client.auth,
        Authentication::Hmac(HmacKey::new(b"CONSPICUOUS_GLOBAL_HMAC_KEY".as_slice()))
    );
    let targets = setup.managed_targets();
    let authentication: Vec<_> = targets.iter().map(|target| target.auth.clone()).collect();
    assert_eq!(
        authentication,
        [
            TargetAuth::Inherit,
            TargetAuth::Override(Authentication::Hmac(HmacKey::new(
                b"CONSPICUOUS_TARGET_HMAC_KEY".as_slice()
            ))),
            TargetAuth::Override(Authentication::Unauthenticated)
        ]
    );
    for output in [format!("{args:?}"), format!("{setup:?}")] {
        assert!(!output.contains("CONSPICUOUS_GLOBAL_HMAC_KEY"));
        assert!(!output.contains("CONSPICUOUS_TARGET_HMAC_KEY"));
    }
}

// A real authenticated server detects broken client argument-to-config mapping.
#[cfg(feature = "client")]
#[test]
fn client_arguments_export_a_real_client_run() {
    use std::{io::Read, process::Stdio, thread};
    let dispatcher = env!("CARGO_BIN_EXE_irtt-rs");
    for option in ["--dscp", "--ttl"] {
        assert!(!Command::new(dispatcher)
            .args(["client", option, "256", "127.0.0.1:9"])
            .output()
            .unwrap()
            .status
            .success());
    }
    let server =
        in_tree_server::InTreeServer::start(ServerConfig::default().with_hmac_key(b"cli-test"));
    let mut client = ClientProcess(
        Command::new(dispatcher)
            .args([
                "client",
                "--duration",
                "100ms",
                "--interval",
                "20ms",
                "--hmac",
                "cli-test",
                "--length",
                "128",
                "--sfill",
                "pattern:ab",
                "--clock",
                "wall",
                "--tstamp",
                "both",
                "--stats",
                "both",
                "--ttl",
                "64",
                "--format",
                "jsonl",
                "--columns",
                "target,event,bytes",
                &format!("edge={}", server.addr),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    // Drain both pipes while waiting so output cannot block client termination.
    let mut stdout = client.0.stdout.take().unwrap();
    let stdout = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let mut stderr = client.0.stderr.take().unwrap();
    let stderr = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let status = client.wait();
    let stdout = stdout.join().unwrap();
    let stderr = stderr.join().unwrap();
    assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
    let stdout = String::from_utf8(stdout).unwrap();
    let rows: Vec<_> = stdout.lines().collect();
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row.starts_with('{')
        && row.ends_with('}')
        && row.contains("\"target\":\"edge\"")));
    assert!(rows
        .iter()
        .any(|row| row.contains("\"event\":\"echo_reply\"") && row.contains("\"bytes\":128")));
    assert!(rows
        .iter()
        .any(|row| row.contains("\"event\":\"session_closed\"")));
}

#[cfg(any(feature = "client", feature = "tui"))]
#[test]
fn malformed_target_diagnostics_do_not_expose_authentication_keys() {
    let secret = "malformed-target-secret-92817";
    for applet in [
        #[cfg(feature = "client")]
        "client",
        #[cfg(feature = "tui")]
        "tui",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_irtt-rs"))
            .args([applet, &format!(r"edge=127.0.0.1:9@hmac={secret}\q")])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            !stderr.trim().is_empty(),
            "malformed target must report a diagnostic"
        );
        assert!(!stderr.contains(secret), "target authentication key leaked");
        assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    }
}

#[cfg(feature = "server")]
#[test]
fn server_arguments_resolve_binds_and_map_policy() {
    use clap::{CommandFactory, FromArgMatches, Parser};
    use irtt_app::cmd::server::ServerArgs;
    use irtt_server::TimestampAllowance;
    use std::time::Duration;
    // Isolate zero-argument defaults from the test runner's environment without
    // mutating process-global environment in this parallel test process.
    let defaults = ServerArgs::from_arg_matches(
        &ServerArgs::command()
            .mut_args(|arg| arg.env(None::<&str>))
            .try_get_matches_from(["irtt-server"])
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        defaults.resolve_binds(),
        vec![
            "[::]:2112".parse().unwrap(),
            "0.0.0.0:2112".parse().unwrap()
        ]
    );
    assert_eq!(
        defaults.server_config(),
        irtt_server::ServerConfig::default()
    );
    let args = ServerArgs::try_parse_from([
        "irtt-server",
        "--bind",
        "127.0.0.1:2113",
        "--bind",
        "[::1]:2114",
        "--hmac",
        "server-cli-test",
        "--max-sessions",
        "3",
        "--max-packet-length",
        "96",
        "--min-interval",
        "25ms",
        "--burst",
        "2",
        "--idle-timeout",
        "8s",
        "--max-duration",
        "1m",
        "--timestamp-allowance",
        "single",
        "--no-dscp",
    ])
    .unwrap();
    assert_eq!(
        args.resolve_binds(),
        vec![
            "127.0.0.1:2113".parse().unwrap(),
            "[::1]:2114".parse().unwrap()
        ]
    );
    let config = args.server_config();
    assert_eq!(config.hmac_key(), Some(b"server-cli-test".as_slice()));
    assert_eq!(config.max_sessions(), 3);
    assert_eq!(config.max_packet_length(), 96);
    assert_eq!(config.min_send_interval(), Duration::from_millis(25));
    assert_eq!(config.burst_allowance(), 2);
    assert_eq!(config.idle_timeout(), Duration::from_secs(8));
    assert_eq!(config.max_test_duration(), Some(Duration::from_secs(60)));
    assert_eq!(config.timestamp_allowance(), TimestampAllowance::Single);
    assert!(!config.dscp_allowed());
}

#[cfg(feature = "server")]
#[test]
fn server_command_line_binds_replace_the_environment_bind() {
    use std::net::UdpSocket;
    // Occupied ports give bounded, observable startup failures, without starting
    // a daemon or needing a process/signal harness merely to inspect arguments.
    let environment = UdpSocket::bind("127.0.0.1:0").unwrap();
    let explicit = UdpSocket::bind("127.0.0.1:0").unwrap();
    let environment_addr = environment.local_addr().unwrap().to_string();
    let explicit_addr = explicit.local_addr().unwrap().to_string();
    for use_explicit in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-server"));
        command
            .env_clear()
            .env("IRTT_SERVER_BIND", &environment_addr)
            .env("IRTT_SERVER_MAX_SESSIONS", "invalid")
            .args(["--max-sessions", "3"]);
        if use_explicit {
            command.args(["--bind", "127.0.0.1:0", "--bind", &explicit_addr]);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        let expected = if use_explicit {
            &explicit_addr
        } else {
            &environment_addr
        };
        assert!(
            stderr.contains(expected),
            "startup must attempt the selected bind: {stderr}"
        );
        assert!(
            !stderr.contains("listening on"),
            "partial startup must not announce listeners"
        );
    }
}

// Exercise stdin replacement and EOF through the actual application process.
#[cfg(feature = "client")]
#[test]
fn targets_stdin_supersedes_the_active_set_and_stops_on_eof() {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        process::Stdio,
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };
    let server = in_tree_server::InTreeServer::start(ServerConfig::default());
    let mut client = ClientProcess(
        Command::new(env!("CARGO_BIN_EXE_irtt-client"))
            .env_clear()
            .args([
                "--duration",
                "0",
                "--targets-stdin",
                "--interval",
                "20ms",
                "--format",
                "jsonl",
                "--columns",
                "target,event",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = client.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let mut stdin = client.0.stdin.take().unwrap();
    let mut rows = Vec::new();
    for target in ["first", "second"] {
        write!(stdin, "\r\n{target}={}\r\n", server.addr).unwrap();
        stdin.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let row = rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("stdin target set did not produce a reply");
            let replied = row.contains(&format!("\"target\":\"{target}\""))
                && row.contains("\"event\":\"echo_reply\"");
            rows.push(row);
            if replied {
                break;
            }
        }
    }
    // Retirement can finish after the newer target's first reply.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rows.iter().any(|row| {
        row.contains("\"target\":\"first\"") && row.contains("\"event\":\"session_closed\"")
    }) {
        rows.push(
            rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("superseded stdin target did not retire"),
        );
    }
    drop(stdin);
    let status = client.wait();
    reader.join().unwrap();
    rows.extend(rx.try_iter());
    let mut stderr = String::new();
    client
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(status.success(), "{stderr}");
    assert!(rows
        .iter()
        .any(|row| row.contains("\"target\":\"second\"")
            && row.contains("\"event\":\"session_closed\"")));
}

// Record bounds and decoding errors must terminate the real stdin frontend,
// even when no managed events are available to wake it.
#[cfg(feature = "client")]
#[test]
fn targets_stdin_invalid_records_are_fatal_and_empty_eof_is_normal() {
    use std::{
        io::{Read, Write},
        process::Stdio,
    };
    for (record, success, diagnostic) in [
        (Vec::new(), true, ""),
        (b"\r\n\n".to_vec(), true, ""),
        (b"\n=127.0.0.1:9\n".to_vec(), false, "line 2"),
        (vec![0xff, b'\n'], false, "line 1"),
        (vec![b'x'; 64 * 1024 + 2], false, "line 1"),
    ] {
        let mut client = ClientProcess(
            Command::new(env!("CARGO_BIN_EXE_irtt-client"))
                .env_clear()
                .args(["--duration", "0", "--targets-stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let mut stdin = client.0.stdin.take().unwrap();
        stdin.write_all(&record).unwrap();
        drop(stdin);
        let status = client.wait();
        let mut stderr = String::new();
        client
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!(status.success(), success, "{stderr}");
        assert!(stderr.contains(diagnostic), "{stderr}");
    }
}
