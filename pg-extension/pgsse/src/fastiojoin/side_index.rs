use std::collections::HashSet;

use itertools::Itertools;
use pgx::{prelude::*, IntoDatum};

use super::{
    crypto::{h1, h2},
    util::xor_bytes,
};

pub fn search_product(
    prefix: &str,
    token: &[u8],
) -> TableIterator<'static, (name!(a_id, i64), name!(b_id, i64))> {
    let (tw, kw, c) = parse_search_token(token);

    let mut a_ids = HashSet::new();
    let mut b_ids = HashSet::new();

    let (a_cached_ids, b_cached_ids) = tc_get(prefix, tw);
    if let Some(ids) = a_cached_ids {
        a_ids.extend(ids);
    }
    if let Some(ids) = b_cached_ids {
        b_ids.extend(ids);
    }

    let kw = match kw {
        Some(v) => v,
        None => {
            return TableIterator::new(
                a_ids
                    .into_iter()
                    .cartesian_product(b_ids.into_iter().collect::<Vec<i64>>()),
            )
        }
    };

    for i in 1..=c {
        let kw_i = concat_kw_i(kw, i);
        let ui = h1(&kw_i);
        let e = match te_get(prefix, &ui) {
            Some(v) => v,
            None => break,
        };
        let (op, side, id) = parse_op_side_id(xor_bytes(e, h2(&kw_i)));

        match side {
            0 => match op {
                0 => a_ids.insert(id),
                1 => a_ids.remove(&id),
                _ => panic!("malformed token: unknown op"),
            },
            1 => match op {
                0 => b_ids.insert(id),
                1 => b_ids.remove(&id),
                _ => panic!("malformed token: unknown op"),
            },
            _ => panic!("malformed token: unknown side"),
        };

        te_delete(prefix, &ui)
    }

    let a_ids: Vec<i64> = a_ids.into_iter().collect();
    let b_ids: Vec<i64> = b_ids.into_iter().collect();

    tc_set(prefix, tw, a_ids.clone(), b_ids.clone());

    TableIterator::new(a_ids.into_iter().cartesian_product(b_ids))
}

fn parse_search_token(token: &[u8]) -> (&[u8], Option<&[u8]>, u64) {
    let tw = &token[..32];
    let has_kw = &token[32..33];
    let has_kw = has_kw[0] == 1;
    let kw = &token[33..49];
    let c = u64::from_be_bytes(token[49..57].try_into().expect("unexpected token size"));

    if has_kw {
        (tw, Some(kw), c)
    } else {
        (tw, None, c)
    }
}

fn te_get(prefix: &str, u: &[u8]) -> Option<Vec<u8>> {
    Spi::get_one_with_args(
        &format!("SELECT e FROM {}_te WHERE u = $1", prefix),
        vec![(PgBuiltInOids::BYTEAOID.oid(), u.into_datum())],
    )
}

fn te_delete(prefix: &str, u: &[u8]) {
    Spi::run_with_args(
        &format!("DELETE FROM {}_te WHERE u = $1", prefix),
        Some(vec![(PgBuiltInOids::BYTEAOID.oid(), u.into_datum())]),
    )
}

fn tc_get(prefix: &str, tw: &[u8]) -> (Option<Vec<i64>>, Option<Vec<i64>>) {
    Spi::get_two_with_args(
        &format!("SELECT a_ids, b_ids FROM {}_tc WHERE tw = $1", prefix),
        vec![(PgBuiltInOids::BYTEAOID.oid(), tw.into_datum())],
    )
}

fn tc_set(prefix: &str, tw: &[u8], a_ids: Vec<i64>, b_ids: Vec<i64>) {
    Spi::run_with_args(
        &format!(
            "INSERT INTO {}_tc VALUES ($1, $2, $3) ON CONFLICT (tw) DO UPDATE SET a_ids = $2, b_ids = $3",
            prefix
        ),
        Some(vec![
            (PgBuiltInOids::BYTEAOID.oid(), tw.into_datum()),
            (PgBuiltInOids::INT8ARRAYOID.oid(), a_ids.into_datum()),
            (PgBuiltInOids::INT8ARRAYOID.oid(), b_ids.into_datum()),
        ]),
    )
}

fn concat_kw_i(kw: &[u8], i: u64) -> Vec<u8> {
    [kw, &i.to_be_bytes()].concat()
}

fn parse_op_side_id(mut op_side_id: Vec<u8>) -> (u8, u8, i64) {
    let op = op_side_id.remove(0);
    let side = op_side_id.remove(0);
    let id = i64::from_be_bytes(op_side_id[..8].try_into().unwrap());

    (op, side, id)
}
