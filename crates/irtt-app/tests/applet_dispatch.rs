use std::process::{Command, Output};

fn irtt_rs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_irtt-rs"))
}

#[cfg(unix)]
fn irtt_rs_with_arg0(arg0: &str) -> Command {
    use std::os::unix::process::CommandExt;

    let mut command = irtt_rs();
    command.arg0(arg0);
    command
}

/// Copies a dedicated binary to `name` in a temp dir and returns a `Command`
/// invoking it under that name, proving dedicated binaries decide their role
/// from the entry point they were built with, not from argv0.
#[cfg(all(unix, any(feature = "client", feature = "server")))]
fn copied_binary(cargo_bin_exe: &str, name: &str) -> Command {
    let dir = std::env::temp_dir().join(format!(
        "irtt-copied-binary-test-{}-{}",
        std::process::id(),
        name
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dest = dir.join(name);
    std::fs::copy(cargo_bin_exe, &dest).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&dest).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dest, perms).unwrap();
    }

    Command::new(dest)
}

fn output_text(output: &Output) -> String {
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

#[test]
fn canonical_help_shows_dispatcher_help() {
    let output = irtt_rs().arg("--help").output().unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("IRTT-compatible multi-applet dispatcher"));
    assert!(text.contains("client"));
    assert!(text.contains("tui"));
    assert!(text.contains("server"));
}

#[test]
fn canonical_without_applet_errors() {
    let output = irtt_rs().output().unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(text.contains("choose an applet"), "{text}");
    assert!(text.contains("client"));
    assert!(text.contains("tui"));
    assert!(text.contains("server"));
}

#[cfg(feature = "client")]
#[test]
fn canonical_client_subcommand_dispatches_to_client() {
    let output = irtt_rs().args(["client", "--help"]).output().unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible stream client"));
    assert!(text.contains("--format <FORMAT>"));
    assert!(text.contains("--columns <COLUMNS>"));
}

#[cfg(not(feature = "client"))]
#[test]
fn canonical_client_subcommand_reports_unavailable_when_disabled() {
    let output = irtt_rs().args(["client", "--help"]).output().unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(
        text.contains("client applet is not available; rebuild with the client feature"),
        "{text}"
    );
}

#[cfg(feature = "server")]
#[test]
fn canonical_server_subcommand_dispatches_to_server() {
    let output = irtt_rs().args(["server", "--help"]).output().unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible UDP server"));
    assert!(text.contains("--bind <ADDR>"));
    assert!(text.contains("--max-sessions <COUNT>"));
    assert!(text.contains("--idle-timeout <DURATION>"));
    assert!(text.contains("--timestamp-allowance <MODE>"));
    assert!(text.contains("--no-dscp"));
}

#[cfg(not(feature = "server"))]
#[test]
fn canonical_server_subcommand_reports_unavailable_when_disabled() {
    let output = irtt_rs().args(["server", "--help"]).output().unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(
        text.contains("server applet is not available; rebuild with the server feature"),
        "{text}"
    );
}

#[cfg(feature = "client")]
#[test]
fn dedicated_client_binary_always_enters_the_client_role() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_irtt-client"))
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible stream client"));
    assert!(text.contains("--format <FORMAT>"));
}

#[cfg(feature = "tui")]
#[test]
fn dedicated_tui_binary_always_enters_the_tui_role() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_irtt-tui"))
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible TUI client"));
    assert!(text.contains("--duration <DURATION>"));
}

#[cfg(feature = "server")]
#[test]
fn dedicated_server_binary_always_enters_the_server_role() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_irtt-server"))
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible UDP server"));
    assert!(text.contains("--bind <ADDR>"));
}

#[cfg(all(unix, feature = "client"))]
#[test]
fn renaming_the_dedicated_client_binary_does_not_change_its_role() {
    let output = copied_binary(env!("CARGO_BIN_EXE_irtt-client"), "whatever")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible stream client"));
}

#[cfg(all(unix, feature = "server"))]
#[test]
fn renaming_the_dedicated_server_binary_does_not_change_its_role() {
    let output = copied_binary(env!("CARGO_BIN_EXE_irtt-server"), "irtt-cli")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible UDP server"));
}

#[cfg(unix)]
#[cfg(feature = "client")]
#[test]
fn client_applet_name_dispatches_to_client() {
    let output = irtt_rs_with_arg0("irtt-client")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible stream client"));
    assert!(text.contains("--format <FORMAT>"));
    assert!(text.contains("--columns <COLUMNS>"));
}

#[cfg(all(unix, not(feature = "client")))]
#[test]
fn client_applet_name_reports_unavailable_when_disabled() {
    let output = irtt_rs_with_arg0("irtt-client")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(
        text.contains("client applet is not available; rebuild with the client feature"),
        "{text}"
    );
}

#[cfg(all(unix, feature = "tui"))]
#[test]
fn tui_applet_name_dispatches_to_tui_when_enabled() {
    let output = irtt_rs_with_arg0("irtt-tui")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible TUI client"));
    assert!(text.contains("--duration <DURATION>"));
}

#[cfg(all(unix, not(feature = "tui")))]
#[test]
fn tui_applet_name_reports_unavailable_when_disabled() {
    let output = irtt_rs_with_arg0("irtt-tui")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(text.contains("TUI applet is not available"), "{text}");
}

#[cfg(all(unix, feature = "server"))]
#[test]
fn server_applet_name_dispatches_to_server_when_enabled() {
    let output = irtt_rs_with_arg0("irtt-server")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Minimal IRTT-compatible UDP server"));
    assert!(text.contains("--bind <ADDR>"));
}

#[cfg(all(unix, not(feature = "server")))]
#[test]
fn server_applet_name_reports_unavailable_when_disabled() {
    let output = irtt_rs_with_arg0("irtt-server")
        .arg("--help")
        .output()
        .unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(text.contains("server applet is not available"), "{text}");
}

#[cfg(unix)]
#[test]
fn unknown_irtt_applet_name_errors() {
    let output = irtt_rs_with_arg0("irtt-typo").output().unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(text.contains("unknown applet name 'irtt-typo'"), "{text}");
    assert!(text.contains("irtt-client"));
    assert!(text.contains("irtt-tui"));
    assert!(text.contains("irtt-server"));
}

#[cfg(unix)]
#[test]
fn old_irtt_cli_binary_name_is_no_longer_a_recognized_applet() {
    let output = irtt_rs_with_arg0("irtt-cli").output().unwrap();
    let text = output_text(&output);

    assert!(!output.status.success());
    assert!(text.contains("unknown applet name 'irtt-cli'"), "{text}");
}
