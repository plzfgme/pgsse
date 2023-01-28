mod fastio;
mod fastiojoin;
mod fastiorange;

#[allow(unused)]
use pgx::prelude::*;

pgx::pg_module_magic!();

// TODO: Add Tests
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
