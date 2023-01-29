use std::collections::HashMap;

use pgx::{prelude::*, IntoDatum};

use super::{
    crypto::{h1, h2_long},
    util::xor_bytes,
};

pub fn search(prefix: &str, token: &[u8]) -> HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)> {
    let (kw, c) = parse_search_token(token);

    let mut token_map = HashMap::new();

    token_map.extend(tc_get(prefix));

    let kw = match kw {
        Some(v) => v,
        None => return token_map,
    };

    let mut token_map2 = HashMap::new();

    for i in 1..=c {
        let kw_i = concat_kw_i(kw, i);
        let ui = h1(&kw_i);
        let e = te_get(prefix, &ui).expect("index broken");
        let (tw, a_token, b_token) = parse_tw_token(xor_bytes(e, h2_long(&kw_i)));
        token_map2.insert(tw, (a_token, b_token));

        te_delete(prefix, &ui);
    }

    token_map.extend(token_map2.clone());

    for (tw, (a_token, b_token)) in token_map2 {
        tc_set(prefix, tw, a_token, b_token);
    }

    token_map
}

fn parse_search_token(token: &[u8]) -> (Option<&[u8]>, u64) {
    let has_kw = &token[0..1];
    let has_kw = has_kw[0] == 1;
    let kw = &token[1..17];
    let c = u64::from_be_bytes(token[17..25].try_into().expect("unexpected token size"));

    if has_kw {
        (Some(kw), c)
    } else {
        (None, c)
    }
}

fn te_get(prefix: &str, u: &[u8]) -> Option<Vec<u8>> {
    Spi::get_one_with_args(
        &format!("SELECT e FROM {}_ab_te WHERE u = $1", prefix),
        vec![(PgBuiltInOids::BYTEAOID.oid(), u.into_datum())],
    )
}

fn te_delete(prefix: &str, u: &[u8]) {
    Spi::run_with_args(
        &format!("DELETE FROM {}_ab_te WHERE u = $1", prefix),
        Some(vec![(PgBuiltInOids::BYTEAOID.oid(), u.into_datum())]),
    )
}

fn tc_get(prefix: &str) -> Vec<(Vec<u8>, (Vec<u8>, Vec<u8>))> {
    Spi::connect(|client| {
        let table = client.select(
            &format!("SELECT tw, a_token, b_token FROM {}_ab_tc", prefix),
            None,
            None,
        );
        Ok(Some(
            table
                .into_iter()
                .map(|tuple| {
                    (
                        tuple.by_ordinal(1).unwrap().value().unwrap(),
                        (
                            tuple.by_ordinal(2).unwrap().value().unwrap(),
                            tuple.by_ordinal(2).unwrap().value().unwrap(),
                        ),
                    )
                })
                .collect(),
        ))
    })
    .unwrap()
}

fn tc_set(prefix: &str, tw: Vec<u8>, a_token: Vec<u8>, b_token: Vec<u8>) {
    Spi::run_with_args(
        &format!(
            "INSERT INTO {}_ab_tc VALUES ($1, $2, $3) ON CONFLICT (tw) DO UPDATE SET a_token = $2, b_token = $3",
            prefix
        ),
        Some(vec![
            (PgBuiltInOids::BYTEAOID.oid(), tw.into_datum()),
            (PgBuiltInOids::BYTEAOID.oid(), a_token.into_datum()),
            (PgBuiltInOids::BYTEAOID.oid(), b_token.into_datum()),
        ]),
    )
}

fn concat_kw_i(kw: &[u8], i: u64) -> Vec<u8> {
    [kw, &i.to_be_bytes()].concat()
}

fn parse_tw_token(op_tw_token: Vec<u8>) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    (
        op_tw_token[..32].into(),
        op_tw_token[32..89].into(),
        op_tw_token[89..146].into(),
    )
}
