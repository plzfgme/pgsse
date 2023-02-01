mod core_index;
mod crypto;
mod side_index;
mod util;

use pgx::prelude::*;

#[pg_extern]
fn pgsse_fastiojoin_search(
    prefix: &str,
    token: &[u8],
) -> TableIterator<'static, (name!(tw, Vec<u8>), name!(a_id, i64), name!(b_id, i64))> {
    let token_map = core_index::search(&format!("{}_core", prefix), token);

    let mut result = Vec::new();

    for (tw, side_token) in token_map.into_iter() {
        result.extend(
            side_index::search_product(&format!("{}_side", prefix), &side_token)
                .map(|(a_id, b_id)| (tw.clone(), a_id, b_id)),
        );
    }

    TableIterator::new(result.into_iter())
}

extension_sql_file!(
    "../sql/fastiojoin.sql",
    name = "fastiojoin",
    requires = ["fastio"]
);
