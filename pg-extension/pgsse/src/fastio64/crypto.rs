use hmac::{Hmac, Mac};
use sha2::Sha256;

pub fn h1(input: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"1").expect("HMAC can take key of any size");
    mac.update(input);

    mac.finalize().into_bytes().to_vec()
}

pub fn h2(input: &[u8]) -> Vec<u8> {
    let mut mac1 = Hmac::<Sha256>::new_from_slice(b"21").expect("HMAC can take key of any size");
    let mut mac2 = Hmac::<Sha256>::new_from_slice(b"22").expect("HMAC can take key of any size");
    mac1.update(input);
    mac2.update(input);

    [mac1.finalize().into_bytes(), mac2.finalize().into_bytes()].concat()
}
