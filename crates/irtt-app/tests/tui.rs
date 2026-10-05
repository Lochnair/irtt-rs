#![cfg(all(unix, feature = "tui"))]

#[path = "support/in_tree_server.rs"]
mod in_tree_server;

use irtt_server::ServerConfig;
use nix::{
    poll::{poll, PollFd, PollFlags},
    pty::{openpty, Winsize},
    sys::{
        signal::{kill, Signal},
        termios::{tcgetattr, Termios},
    },
    unistd::Pid,
};
use std::{
    fs::File,
    io::{Read, Write},
    os::{
        fd::{AsFd, AsRawFd},
        unix::process::CommandExt,
    },
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

// Real terminal ownership: keep both ends open to inspect restoration after exit,
// drain output while waiting, and always reap the child on assertion failures.
struct TuiProcess {
    child: Child,
    master: File,
    slave: File,
    original: Termios,
    output: Vec<u8>,
    cursor_answered: bool,
}
impl TuiProcess {
    fn start(mut command: Command) -> Self {
        let pty = openpty(
            Some(&Winsize {
                ws_row: 40,
                ws_col: 160,
                ws_xpixel: 0,
                ws_ypixel: 0,
            }),
            None,
        )
        .unwrap();
        let slave = File::from(pty.slave);
        let original = tcgetattr(&slave).unwrap();
        command
            .env("TERM", "xterm-256color")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()));
        // Only async-signal-safe operations run between fork and exec.
        unsafe {
            command.pre_exec(|| {
                if nix::libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Self {
            child: command.spawn().unwrap(),
            master: File::from(pty.master),
            slave,
            original,
            output: vec![],
            cursor_answered: false,
        }
    }
    fn pump(&mut self) {
        let mut fd = [PollFd::new(self.master.as_fd(), PollFlags::POLLIN)];
        poll(&mut fd, 20_u16).unwrap();
        if fd[0].revents().unwrap().contains(PollFlags::POLLIN) {
            let mut buffer = [0; 65536];
            if let Ok(count) = self.master.read(&mut buffer) {
                self.output.extend_from_slice(&buffer[..count]);
                if !self.cursor_answered && self.output.windows(4).any(|bytes| bytes == b"\x1b[6n")
                {
                    self.master.write_all(b"\x1b[1;1R").unwrap();
                    self.cursor_answered = true;
                }
            }
        }
    }
    fn await_text(&mut self, text: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !String::from_utf8_lossy(&self.output).contains(text) {
            self.pump();
            assert!(
                Instant::now() < deadline,
                "missing {text:?}: {}",
                String::from_utf8_lossy(&self.output)
            );
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "TUI exited before {text:?}: {}",
                String::from_utf8_lossy(&self.output)
            );
        }
    }
    fn keys(&mut self, keys: &[u8]) {
        self.master.write_all(keys).unwrap();
    }
    fn signal(&self, signal: Signal) {
        kill(Pid::from_raw(self.child.id() as i32), signal).unwrap();
    }
    fn finish(&mut self, success: bool) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            self.pump();
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "TUI did not finish: {}",
                String::from_utf8_lossy(&self.output[self.output.len().saturating_sub(3000)..])
            );
        };
        self.pump();
        assert_eq!(
            status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&self.output)
        );
        let restored = tcgetattr(&self.master).unwrap();
        assert_eq!(restored, self.original, "raw mode was not restored");
        let output = String::from_utf8_lossy(&self.output);
        assert!(
            output.contains("\x1b[?1049l"),
            "alternate screen was not left: {output}"
        );
        assert!(
            output.contains("\x1b[?25h"),
            "cursor was not restored: {output}"
        );
        status
    }
}
impl Drop for TuiProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn tui(target: &str, duration: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_irtt-tui"));
    cmd.args(["--duration", duration, "--interval", "20ms", target]);
    cmd
}

#[test]
fn terminal_controls_and_exit_paths_restore_the_terminal() {
    let server = in_tree_server::InTreeServer::start(ServerConfig::default());
    let target = server.addr.to_string();
    let mut command = tui(&format!("first={target}"), "0");
    command.arg(format!("second={target}"));
    let mut process = TuiProcess::start(command);
    process.await_text("active");
    process.keys(b"p");
    process.await_text("paused");
    let size = Winsize {
        ws_row: 12,
        ws_col: 50,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe { nix::libc::ioctl(process.slave.as_raw_fd(), nix::libc::TIOCSWINSZ, &size) },
        0
    );
    process.signal(Signal::SIGWINCH);
    process.await_text("small");
    let size = Winsize {
        ws_row: 40,
        ws_col: 160,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe { nix::libc::ioctl(process.slave.as_raw_fd(), nix::libc::TIOCSWINSZ, &size) },
        0
    );
    process.signal(Signal::SIGWINCH);
    process.keys(b"\td");
    process.await_text("arrows");
    // Each key waits for a real draw, avoiding a timing-dependent escape batch.
    for key in [
        b"\x1b[B".as_slice(),
        b"\x1b[C",
        b"\x1b[6~",
        b"\x1b[F",
        b"\x1b[H",
        b"d",
        b"m",
        b"+",
        b"-",
        b"0",
        b"\x1b[D",
        b"\x1b[C",
        b"\x1b[5~",
        b"\x1b[6~",
        b"\x1b[H",
        b"\x1b[F",
        b"r",
        b"p",
    ] {
        let frames = process
            .output
            .windows(6)
            .filter(|bytes| *bytes == b"\x1b[?25l")
            .count();
        process.keys(key);
        let deadline = Instant::now() + Duration::from_secs(5);
        while process
            .output
            .windows(6)
            .filter(|bytes| *bytes == b"\x1b[?25l")
            .count()
            == frames
        {
            process.pump();
            assert!(Instant::now() < deadline, "input did not redraw: {key:?}");
        }
    }
    process.keys(b"q");
    process.finish(true);

    for action in [None, Some(b"\x03".as_slice()), Some(b"signal".as_slice())] {
        let mut process =
            TuiProcess::start(tui(&target, if action.is_none() { "100ms" } else { "0" }));
        process.await_text("active");
        match action {
            Some(b"signal") => process.signal(Signal::SIGTERM),
            Some(keys) => process.keys(keys),
            None => {}
        }
        process.finish(true);
    }
    // An adverse early-close proxy forwards the real Rust server's OPEN and
    // echo bytes. Only the documented CLOSE flag is added to the first echo.
    // This exercises the application's continuous peer-close exit policy.
    std::thread::scope(|scope| {
        use irtt_proto::{decode_request, DecodedRequestKind, FLAG_CLOSE};
        use std::net::UdpSocket;
        let proxy = UdpSocket::bind("127.0.0.1:0").unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        proxy
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let upstream = UdpSocket::bind("127.0.0.1:0").unwrap();
        upstream.connect(server.addr).unwrap();
        upstream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let peer = scope.spawn(move || {
            let mut packet = [0; 65536];
            loop {
                let (count, client) = proxy.recv_from(&mut packet).unwrap();
                let echo = matches!(
                    decode_request(&packet[..count]).unwrap().kind,
                    DecodedRequestKind::Echo { .. }
                );
                upstream.send(&packet[..count]).unwrap();
                let count = upstream.recv(&mut packet).unwrap();
                if echo {
                    packet[3] |= FLAG_CLOSE;
                }
                proxy.send_to(&packet[..count], client).unwrap();
                if echo {
                    break;
                }
            }
        });
        let mut process = TuiProcess::start(tui(&proxy_addr.to_string(), "0"));
        process.finish(false);
        assert!(String::from_utf8_lossy(&process.output)
            .contains("continuous run ended because of peer closure"));
        peer.join().unwrap();
    });

    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "cancelled_frontend_fixture", "--nocapture"])
        .env("IRTT_TUI_CANCEL_TARGET", &target);
    TuiProcess::start(command).finish(true);

    // Opening failure must be reported and restore the terminal too.
    let mut process = TuiProcess::start(tui("127.0.0.1:99999", "100ms"));
    process.finish(false);
    assert!(String::from_utf8_lossy(&process.output)
        .contains("no managed target completed successfully"));
}

// The parent runs this fixture in its own PTY. Dropping the
// actual frontend future must restore that terminal and join measurement.
#[test]
fn cancelled_frontend_fixture() {
    let Ok(target) = std::env::var("IRTT_TUI_CANCEL_TARGET") else {
        return;
    };
    use clap::Parser;
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let args = irtt_app::cmd::tui::TuiArgs::parse_from(["irtt-tui", "--duration", "0", &target]);
    let (_shutdown, receiver) = tokio::sync::watch::channel(false);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut frontend = Box::pin(irtt_app::cmd::tui::run_tui(args, receiver));
            poll_fn(|cx| {
                assert!(frontend.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(frontend);
        });
}
