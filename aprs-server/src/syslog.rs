//! Syslog logging layer for tracing, using libc's POSIX syslog(3).
#![cfg(unix)]

use std::ffi::CString;
use tracing::Level;
use tracing_subscriber::Layer;

/// Maps tracing levels to syslog priorities (RFC 5424).
fn level_to_priority(level: &Level) -> libc::c_int {
    match *level {
        Level::ERROR => libc::LOG_ERR,
        Level::WARN => libc::LOG_WARNING,
        Level::INFO => libc::LOG_INFO,
        Level::DEBUG => libc::LOG_DEBUG,
        Level::TRACE => libc::LOG_DEBUG,
    }
}

/// A tracing Layer that sends log messages to the system syslog.
///
/// Uses libc `openlog`/`syslog`/`closelog` directly for portability
/// across Linux and FreeBSD (avoids platform-specific socket paths).
pub struct SyslogLayer {
    /// Kept alive because openlog stores the pointer — must not drop.
    _ident: CString,
}

impl SyslogLayer {
    pub fn new(ident: &str) -> Self {
        let ident = CString::new(ident).expect("syslog ident must not contain NUL");
        unsafe {
            libc::openlog(
                ident.as_ptr(),
                libc::LOG_PID | libc::LOG_NDELAY,
                libc::LOG_DAEMON,
            );
        }
        SyslogLayer { _ident: ident }
    }
}

impl Drop for SyslogLayer {
    fn drop(&mut self) {
        unsafe {
            libc::closelog();
        }
    }
}

/// Visitor that extracts the formatted message from tracing events.
struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        MessageVisitor {
            message: String::new(),
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for SyslogLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        if visitor.message.is_empty() {
            return;
        }

        let priority = level_to_priority(event.metadata().level());

        // Strip NUL bytes to avoid truncation in CString
        let clean = visitor.message.replace('\0', "");
        if let Ok(msg) = CString::new(clean) {
            unsafe {
                // Use "%s" format to prevent format-string injection
                libc::syslog(priority, b"%s\0".as_ptr() as *const libc::c_char, msg.as_ptr());
            }
        }
    }
}
