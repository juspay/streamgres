//! TLS on the connections to Postgres, governed by the DSN's `sslmode`.
//!
//! Without it, the DSN's password and every row the change feed carries
//! cross the network readable by whoever shares it. Every connection the
//! server opens — the storage pool's, the catalog and slot checks', the
//! change feed's and the snapshot-minting one — follows the setting the
//! connection string names:
//!
//! - `disable`: never encrypted;
//! - `prefer` (the default): encrypted when the server offers TLS, plain
//!   when it does not;
//! - `require`: encrypted, or refused rather than silently plain.
//!
//! Encryption is protection against a passive listener: below the
//! `verify-*` modes (which tokio-postgres's parser has no values for and
//! this module does not read) the certificate is not checked — any
//! server's is accepted, so the connection is encrypted without the
//! server's identity being pinned to a root. An operator on a network
//! where an active attacker matters should take that as: TLS here keeps
//! the wire unreadable, not the server's identity proven.
//!
//! Unix-socket connections carry no TLS: the bytes never leave the host.
//!
//! The mode tokio-postgres's `Config` parser already read is reused by
//! every consumer: tokio-postgres's own `sslmode` handling (try TLS,
//! fall back when `prefer` allows) drives the queries' connector
//! ([`Tls::for_queries`]), and the change feed's transport
//! ([`Tls::for_feed`]) plus the protocol the snapshot-minting connection
//! speaks by hand ([`Tls::for_minting`], see [`super::replication`])
//! follow the same mode in their own negotiations.

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;

use crate::sync::storage::StorageError;

/// The TLS posture of one DSN: the mode its `Config` already carries and
/// one shared rustls client config for every connection the server opens
/// to that database.
#[derive(Clone)]
pub struct Tls {
    config: Arc<ClientConfig>,
}

impl Tls {
    /// Build the TLS settings every connection to the database shares.
    /// The mode is never carried here: each consumer reads it from the
    /// `Config` it connects with ([`Tls::for_feed`],
    /// [`Tls::for_minting`]), whose parser accepts `disable`, `prefer`
    /// and `require`, the modes this module follows. The rustls config
    /// accepts any certificate (see the module docs), pinned to the
    /// `ring` provider the rest of the tree builds, since the dependency
    /// set enables more than one provider and rustls refuses to choose.
    pub(crate) fn new() -> Self {
        let mut config = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("the ring provider speaks TLS 1.2 and 1.3")
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(AnyCertificate));
        Tls {
            config: Arc::new(config),
        }
    }

    /// The connector tokio-postgres's connections are opened with —
    /// the queries' pool, the catalog's and the slot checks'. Its
    /// `connect_tls` reads the mode from the config it is given, so
    /// `disable`/`prefer`/`require` there are followed as-is.
    pub(crate) fn for_queries(&self) -> tokio_postgres_rustls::MakeRustlsConnect {
        tokio_postgres_rustls::MakeRustlsConnect::new((*self.config).clone())
    }

    /// The TLS posture for the change feed: its transport does the SSL
    /// negotiation itself, asked to follow the same mode the config
    /// carries. pgwire-replication's `TlsConfig` has no field for an
    /// external `Arc<ClientConfig>` — it builds its own rustls config
    /// internally (with its own accept-any verifier, the same posture
    /// `AnyCertificate` holds below) — so the mode alone crosses; only a
    /// forked `TlsConfig` could make the feed share `self.config`.
    pub(crate) fn for_feed(&self, config: &Config) -> pgwire_replication::TlsConfig {
        let mode = match config.get_ssl_mode() {
            SslMode::Disable => pgwire_replication::SslMode::Disable,
            // A mode the parser grew past the three read here is the
            // operator asking for at least encryption: `require`.
            SslMode::Prefer => pgwire_replication::SslMode::Prefer,
            _ => pgwire_replication::SslMode::Require,
        };
        pgwire_replication::TlsConfig {
            mode,
            ..Default::default()
        }
    }

    /// The TLS client settings the snapshot-minting connection speaks
    /// with, plus the mode, so its own SSL request knows whether a
    /// server that answers "no TLS" ends the connection.
    pub(crate) fn for_minting(&self, config: &Config) -> Minting {
        Minting {
            config: self.config.clone(),
            mode: config.get_ssl_mode(),
        }
    }
}

/// [`Tls`]'s halves for the connection that speaks the protocol itself:
/// the rustls settings, and the mode, so a refused TLS upgrade is a
/// failure only when the DSN said `require`.
pub(crate) struct Minting {
    pub config: Arc<ClientConfig>,
    mode: SslMode,
}

impl Minting {
    /// Whether a server that cannot do TLS ends the connection: every
    /// mode but `disable` and `prefer` forbids the fallback to
    /// plaintext — phrased as the negation so a mode tokio-postgres
    /// grows past the three read here fails safe, the way
    /// [`Tls::for_feed`]'s catch-all maps it to `require`.
    pub(crate) fn refused_tls_is_fatal(&self) -> bool {
        !matches!(self.mode, SslMode::Disable | SslMode::Prefer)
    }

    /// Whether the DSN said `sslmode=disable`: the SSL request is not
    /// even sent.
    pub(crate) fn ssl_disabled(&self) -> bool {
        self.mode == SslMode::Disable
    }
}

/// The verifier every mode uses: any certificate is accepted — the
/// connection is encrypted without the server's identity being checked
/// against any root (see the module docs).
#[derive(Debug)]
struct AnyCertificate;

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// Whether a credential may cross the connection in readable form:
/// never over a plain TCP connection — a server asking for one there
/// would have it read by every hop on the path, so the connection is
/// refused instead — and always over TLS or a Unix socket, where it
/// does not leave the host or does not leave it readable.
pub fn may_send_cleartext_password(encrypted: bool, unix: bool) -> bool {
    encrypted || unix
}

/// The refusal the check above arms, worded for the log.
pub fn cleartext_password_refused() -> StorageError {
    StorageError(
        "the server asked for the password in cleartext, but this connection is \
         neither encrypted nor local; set the server to SCRAM-SHA-256, or ask for \
         sslmode=require in the connection string"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cleartext password crosses TLS and Unix sockets and nothing
    /// else.
    #[test]
    fn the_cleartext_password_needs_shelter() {
        assert!(!may_send_cleartext_password(false, false));
        assert!(may_send_cleartext_password(true, false));
        assert!(may_send_cleartext_password(false, true));
    }

    /// The minting connection's fallback follows the config's mode:
    /// only `require` refuses a server that cannot do TLS, and only
    /// `disable` never asks.
    #[test]
    fn the_mintings_fallback_follows_the_mode() {
        let tls = Tls::new();
        for (mode, fatal, disabled) in [
            (SslMode::Disable, false, true),
            (SslMode::Prefer, false, false),
            (SslMode::Require, true, false),
        ] {
            let mut config = Config::new();
            config.ssl_mode(mode);
            let minting = tls.for_minting(&config);
            assert_eq!(minting.refused_tls_is_fatal(), fatal, "{mode:?}");
            assert_eq!(minting.ssl_disabled(), disabled, "{mode:?}");
        }
    }

    /// The feed is handed the mode the config carries, in the
    /// transport's own vocabulary.
    #[test]
    fn the_feed_follows_the_mode() {
        let tls = Tls::new();
        for (mode, expected) in [
            (SslMode::Disable, pgwire_replication::SslMode::Disable),
            (SslMode::Prefer, pgwire_replication::SslMode::Prefer),
            (SslMode::Require, pgwire_replication::SslMode::Require),
        ] {
            let mut config = Config::new();
            config.ssl_mode(mode);
            assert_eq!(tls.for_feed(&config).mode, expected);
        }
    }
}
