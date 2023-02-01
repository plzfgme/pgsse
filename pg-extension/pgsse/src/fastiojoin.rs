use std::collections::HashMap;

use itertools::Itertools;
use pgx::prelude::*;

use crate::fastio64::search;

#[pg_extern]
fn pgsse_fastiojoin_search(
    prefix: &str,
    token: &[u8],
) -> TableIterator<'static, (name!(tw, Vec<u8>), name!(a_id, i64), name!(b_id, i64))> {
    let mut id_map = HashMap::new();

    for b in search(prefix, token) {
        let (tw, side, id) = parse_tw_side_id(b);
        match id_map.get_mut(&tw) {
            None => {
                id_map.insert(tw, (Vec::new(), Vec::new()));
            }
            Some((a_ids, b_ids)) => match side {
                0 => a_ids.push(id),
                1 => b_ids.push(id),
                _ => panic!("unknown side"),
            },
        }
    }

    let mut result = Vec::new();

    for (tw, (a_ids, b_ids)) in id_map.into_iter() {
        result.extend(
            a_ids
                .into_iter()
                .cartesian_product(b_ids)
                .map(|(a_id, b_id)| (tw.clone(), a_id, b_id)),
        )
    }

    TableIterator::new(result.into_iter())
}

fn parse_tw_side_id(b: Vec<u8>) -> (Vec<u8>, u8, i64) {
    (
        b[..32].to_vec(),
        b[32],
        i64::from_be_bytes(b[33..41].try_into().unwrap()),
    )
}

extension_sql_file!(
    "../sql/fastiojoin.sql",
    name = "fastiojoin",
    requires = ["fastio64"]
);
