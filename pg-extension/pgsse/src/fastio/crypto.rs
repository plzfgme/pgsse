use hmac::{Hmac, Mac};
use sha2::Sha256;

pub fn h1(input: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"1").expect("HMAC can take key of any size");
    mac.update(input);

    mac.finalize().into_bytes().to_vec()
}

pub fn h2(input: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"2").expect("HMAC can take key of any size");
    mac.update(input);

    mac.finalize().into_bytes().to_vec()
}
