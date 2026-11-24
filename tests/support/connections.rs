//! Explicit application connection factory for live integration tests.
//! Production applications should supply their existing bounded pool instead.
pub fn connections(dsn: &str) -> impl streamgres::cache::Connections {
    let dsn = dsn.to_owned();
    move || {
        let dsn = dsn.clone();
        async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| error.to_string())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(streamgres::cache::Lease::new(client))
        }
    }
}
