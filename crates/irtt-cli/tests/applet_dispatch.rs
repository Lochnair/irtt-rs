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

#[cfg(unix)]
#[cfg(feature = "client")]
#[test]
fn client_applet_name_dispatches_to_client() {
    let output = irtt_rs_with_arg0("irtt-cli")
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
    let output = irtt_rs_with_arg0("irtt-cli")
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

#[cfg(unix)]
#[test]
fn server_applet_name_reports_unavailable() {
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
    assert!(text.contains("irtt-cli"));
    assert!(text.contains("irtt-tui"));
    assert!(text.contains("irtt-server"));
}
