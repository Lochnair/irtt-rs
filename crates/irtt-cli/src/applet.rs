use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedApplet {
    Client,
    Tui,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppletAvailability {
    Client,
    Tui,
    Server,
    None,
}

pub fn detect_applet_from_argv0(argv0: &str) -> Option<RequestedApplet> {
    match Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(argv0)
    {
        "irtt-cli" => Some(RequestedApplet::Client),
        "irtt-tui" => Some(RequestedApplet::Tui),
        "irttd" | "irtt-server" => Some(RequestedApplet::Server),
        "irtt-rs" => None,
        _ => None,
    }
}

pub const fn default_applet_for_features(
    client_enabled: bool,
    tui_enabled: bool,
    server_enabled: bool,
) -> AppletAvailability {
    if client_enabled {
        AppletAvailability::Client
    } else if tui_enabled {
        AppletAvailability::Tui
    } else if server_enabled {
        AppletAvailability::Server
    } else {
        AppletAvailability::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applet_detection_maps_binary_names() {
        assert_eq!(
            detect_applet_from_argv0("/usr/bin/irtt-cli"),
            Some(RequestedApplet::Client)
        );
        assert_eq!(
            detect_applet_from_argv0("irtt-tui"),
            Some(RequestedApplet::Tui)
        );
        assert_eq!(
            detect_applet_from_argv0("irtt-server"),
            Some(RequestedApplet::Server)
        );
        assert_eq!(detect_applet_from_argv0("irtt-rs"), None);
        assert_eq!(detect_applet_from_argv0("custom-name"), None);
    }

    #[test]
    fn default_applet_prefers_client_then_tui() {
        assert_eq!(
            default_applet_for_features(true, true, false),
            AppletAvailability::Client
        );
        assert_eq!(
            default_applet_for_features(false, true, false),
            AppletAvailability::Tui
        );
        assert_eq!(
            default_applet_for_features(false, false, false),
            AppletAvailability::None
        );
    }
}
