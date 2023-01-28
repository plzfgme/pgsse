use pgx::prelude::*;

use crate::fastio;

#[pg_extern]
fn pgsse_fastiorange_search(prefix: &str, token: Vec<&[u8]>) -> SetOfIterator<'static, i64> {
    let mut iter_vec = Vec::new();
    for fastio_token in token {
        iter_vec.push(fastio::search(prefix, fastio_token));
    }
    let bytes_iter = iter_vec.into_iter().flatten();

    SetOfIterator::new(bytes_iter.map(|b| i64::from_be_bytes(b[..8].try_into().unwrap())))
}

extension_sql_file!(
    "../sql/fastiorange.sql",
    name = "fastiorange",
    requires = ["fastio"]
);
