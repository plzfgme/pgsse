pub fn xor_bytes<A, B>(a: A, b: B) -> Vec<u8>
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
