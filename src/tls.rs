//! TLS settings for [`crate::connect`].
//!
//! TLS is the default: certificates are verified against the platform root
//! store and the hostname in [`crate::Config::host`] is checked. Callers who
//! use a private certificate authority supply its certificates through
//! [`Tls::RootCa`]; callers who need client certificates or non-default
//! ciphers supply a complete [`rustls::ClientConfig`] through
//! [`Tls::Custom`].

use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};

/// TLS settings. Ignored when [`crate::Config::plaintext`] is set.
#[derive(Clone, Default)]
pub enum Tls {
    /// Verify against the platform root store with default settings.
    #[default]
    System,
    /// Verify against the certificates supplied here instead of the system
    /// store.
    RootCa(Vec<rustls::pki_types::CertificateDer<'static>>),
    /// Use the supplied rustls client configuration as-is.
    Custom(Arc<rustls::ClientConfig>),
}

impl Tls {
    /// Build [`Tls::System`] explicitly.
    #[must_use]
    pub fn system() -> Self {
        Tls::System
    }

    /// Verify against PEM-encoded certificates.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tls`] when the PEM holds no certificate, or when a
    /// certificate in it cannot be parsed.
    pub fn root_ca_pem(pem: &[u8]) -> Result<Self, Error> {
        let mut certs: Vec<rustls::pki_types::CertificateDer<'static>> = Vec::new();
        for cert in rustls_pemfile::certs(&mut &pem[..]) {
            match cert {
                Ok(cert) => certs.push(cert.into_owned()),
                Err(error) => {
                    return Err(Error::Tls {
                        detail: format!("root CA PEM could not be parsed: {error}"),
                    });
                }
            }
        }
        if certs.is_empty() {
            return Err(Error::Config {
                detail: "root CA PEM contains no certificates".to_owned(),
            });
        }
        Ok(Tls::RootCa(certs))
    }

    /// Verify against a DER-encoded certificate.
    #[must_use]
    pub fn root_ca_der(der: Vec<u8>) -> Self {
        Tls::RootCa(vec![rustls::pki_types::CertificateDer::from(der)])
    }

    pub(crate) fn client_config(&self) -> Result<Arc<rustls::ClientConfig>, Error> {
        match self {
            Tls::System => {
                let roots = load_system_roots()?;
                Ok(Arc::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                ))
            }
            Tls::RootCa(certs) => {
                if certs.is_empty() {
                    return Err(Error::Tls {
                        detail: "root CA list is empty".to_owned(),
                    });
                }
                let mut roots = rustls::RootCertStore::empty();
                for cert in certs.clone() {
                    roots.add(cert).map_err(|error| Error::Tls {
                        detail: format!("root CA certificate could not be parsed: {error}"),
                    })?;
                }
                Ok(Arc::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                ))
            }
            Tls::Custom(config) => Ok(Arc::clone(config)),
        }
    }
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tls::System => f.write_str("System"),
            Tls::RootCa(certs) => f
                .debug_struct("RootCa")
                .field("certificates", &certs.len())
                .finish_non_exhaustive(),
            Tls::Custom(_) => f.write_str("Custom"),
        }
    }
}

fn load_system_roots() -> Result<rustls::RootCertStore, Error> {
    let result = rustls_native_certs::load_native_certs();
    if !result.errors.is_empty() {
        return Err(Error::Tls {
            detail: "could not load the platform root store".to_owned(),
        });
    }
    if result.certs.is_empty() {
        return Err(Error::Tls {
            detail: "the platform root store is empty".to_owned(),
        });
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in result.certs {
        roots.add(cert).map_err(|error| Error::Tls {
            detail: format!("a platform root certificate could not be parsed: {error}"),
        })?;
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_is_the_default() {
        assert!(matches!(Tls::default(), Tls::System));
    }

    #[test]
    fn an_empty_root_ca_is_rejected() {
        let error = Tls::RootCa(Vec::new()).client_config().unwrap_err();
        assert!(matches!(error, Error::Tls { .. }));
    }

    #[test]
    fn an_unparseable_certificate_is_rejected() {
        let error = Tls::root_ca_der(b"not a certificate".to_vec())
            .client_config()
            .unwrap_err();
        assert!(matches!(error, Error::Tls { .. }));
    }

    #[test]
    fn debug_never_prints_certificate_bytes() {
        let tls = Tls::root_ca_der(b"-----BEGIN CERTIFICATE-----".to_vec());
        let rendered = format!("{tls:?}");
        assert!(!rendered.contains("BEGIN"));
    }
}
