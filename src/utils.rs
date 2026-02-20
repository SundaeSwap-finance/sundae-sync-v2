use std::time::{Duration, SystemTime};

pub fn elapsed(start: SystemTime) -> Duration {
    SystemTime::now()
        .duration_since(start)
        .expect("time went backwards")
}

pub fn u64_to_bigint(value: u64) -> utxorpc::spec::cardano::BigInt {
    utxorpc::spec::cardano::BigInt {
        big_int: Some(utxorpc::spec::cardano::big_int::BigInt::Int(value as i64)),
    }
}

pub fn bigint_to_u64(value: &utxorpc::spec::cardano::BigInt) -> Option<u64> {
    if let utxorpc::spec::cardano::big_int::BigInt::Int(val) = value.big_int.as_ref()? {
        return Some(*val as u64);
    };
    None
}
