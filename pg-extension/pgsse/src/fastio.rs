mod crypto;

use std::collections::HashSet;

use pgx::{prelude::*, IntoDatum};

use self::crypto::{h1, h2};

#[pg_extern]
fn pgsse_fastio_search(prefix: &str, token: &[u8]) -> SetOfIterator<'static, i64> {
    let bytes_iter = search(prefix, token);

    SetOfIterator::new(bytes_iter.map(|b| i64::from_be_bytes(b[..8].try_into().unwrap())))
}

pub(crate) fn search(prefix: &str, token: &[u8]) -> SetOfIterator<'static, Vec<u8>> {
    let (tw, kw, c) = parse_search_token(token);

    let mut ids = HashSet::new();

    if let Some(cached_ids) = tc_get(prefix, tw) {
        ids.extend(cached_ids);
    }

    let kw = match kw {
        Some(v) => v,
        None => return SetOfIterator::new(ids),
    };

    for i in 1..=c {
        let kw_i = concat_kw_i(kw, i);
        let ui = h1(&kw_i);
        let e = match te_get(prefix, &ui) {
            Some(v) => v,
            None => break,
        };
        let (op, id) = parse_op_id(xor_bytes(e, h2(&kw_i)));

        if op == 1 {
            // op == "del"
            ids.remove(&id);
        } else {
            ids.insert(id);
        }

        te_delete(prefix, &ui)
    }

    let ids: Vec<Vec<u8>> = ids.into_iter().collect();

    tc_set(prefix, tw, ids.clone());

    SetOfIterator::new(ids)
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
    .unwrap()
}

fn te_delete(prefix: &str, u: &[u8]) {
    Spi::run_with_args(
        &format!("DELETE FROM {}_te WHERE u = $1", prefix),
        Some(vec![(PgBuiltInOids::BYTEAOID.oid(), u.into_datum())]),
    )
    .unwrap();
}

fn tc_get(prefix: &str, tw: &[u8]) -> Option<Vec<Vec<u8>>> {
    Spi::get_one_with_args(
        &format!("SELECT ids FROM {}_tc WHERE tw = $1", prefix),
        vec![(PgBuiltInOids::BYTEAOID.oid(), tw.into_datum())],
    )
    .unwrap()
}

fn tc_set(prefix: &str, tw: &[u8], ids: Vec<Vec<u8>>) {
    Spi::run_with_args(
        &format!(
            "INSERT INTO {}_tc VALUES ($1, $2) ON CONFLICT (tw) DO UPDATE SET ids = $2",
            prefix
        ),
        Some(vec![
            (PgBuiltInOids::BYTEAOID.oid(), tw.into_datum()),
            (PgBuiltInOids::BYTEAARRAYOID.oid(), ids.into_datum()),
        ]),
    )
    .unwrap();
}

fn concat_kw_i(kw: &[u8], i: u64) -> Vec<u8> {
    [kw, &i.to_be_bytes()].concat()
}

fn parse_op_id(mut op_id: Vec<u8>) -> (u8, Vec<u8>) {
    let op = op_id.remove(0);

    (op, op_id)
}

fn xor_bytes<A, B>(a: A, b: B) -> Vec<u8>
where
    A: AsRef<[u8]>,
    B: AsRef<[u8]>,
{
    a.as_ref()
        .iter()
        .zip(b.as_ref().iter())
        .map(|(&x, &y)| x ^ y)
        .collect()
}

extension_sql_file!("../sql/fastio.sql", name = "fastio");
