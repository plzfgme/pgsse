mod core_index;
mod crypto;
mod side_index;
mod util;

use pgx::prelude::*;

use itertools::Itertools;

#[pg_extern]
fn pgsse_fastiojoin_search(
    prefix: &str,
    token: &[u8],
) -> TableIterator<'static, (name!(a_id, i64), name!(b_id, i64), name!(tw, Vec<u8>))> {
    let token_map = core_index::search(prefix, token);
    let mut result = Vec::new();

    for (tw, (a_token, b_token)) in token_map.into_iter() {
        let a_ids = side_index::search(&format!("{}_a", prefix), &a_token);
        let b_ids = side_index::search(&format!("{}_b", prefix), &b_token);
        let b_ids_vec: Vec<_> = b_ids.collect();

        let rows = a_ids
            .cartesian_product(b_ids_vec)
            .map(|(a_id, b_id)| {
                (
                    i64::from_be_bytes(a_id[..8].try_into().unwrap()),
                    i64::from_be_bytes(b_id[..8].try_into().unwrap()),
                )
            })
            .map(|(a_id, b_id)| (a_id, b_id, tw.clone()));

        result.extend(rows);
    }

    TableIterator::new(result.into_iter())
}
