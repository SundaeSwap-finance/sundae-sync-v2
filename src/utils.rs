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

pub fn bigint_to_string(value: &utxorpc::spec::cardano::BigInt) -> String {
    use num_bigint::{BigInt, Sign};
    use utxorpc::spec::cardano::big_int::BigInt as UtxorpcBigInt;
    let value = match &value.big_int {
        Some(UtxorpcBigInt::Int(i)) => BigInt::from_signed_bytes_le(&i.to_le_bytes()),
        Some(UtxorpcBigInt::BigUInt(i)) => BigInt::from_bytes_be(Sign::Plus, i),
        Some(UtxorpcBigInt::BigNInt(i)) => BigInt::from_bytes_be(Sign::Minus, i) - 1,
        None => BigInt::default(),
    };
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use utxorpc::spec::cardano::{big_int, BigInt};

    fn make(inner: big_int::BigInt) -> BigInt {
        BigInt {
            big_int: Some(inner),
        }
    }

    #[test]
    fn none_is_zero() {
        assert_eq!(bigint_to_string(&BigInt { big_int: None }), "0");
    }

    #[test]
    fn int_zero() {
        assert_eq!(bigint_to_string(&make(big_int::BigInt::Int(0))), "0");
    }

    #[test]
    fn int_positive() {
        assert_eq!(bigint_to_string(&make(big_int::BigInt::Int(42))), "42");
    }

    #[test]
    fn int_negative() {
        assert_eq!(bigint_to_string(&make(big_int::BigInt::Int(-42))), "-42");
    }

    #[test]
    fn int_max() {
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::Int(i64::MAX))),
            "9223372036854775807"
        );
    }

    #[test]
    fn int_min() {
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::Int(i64::MIN))),
            "-9223372036854775808"
        );
    }

    #[test]
    fn biguint_small() {
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigUInt(vec![0x01].into()))),
            "1"
        );
    }

    #[test]
    fn biguint_exceeds_u64() {
        // 2^64 = [0x01, 0x00 x8]
        let bytes = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigUInt(bytes.into()))),
            "18446744073709551616"
        );
    }

    #[test]
    fn biguint_empty_bytes_is_zero() {
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigUInt(vec![].into()))),
            "0"
        );
    }

    // BigNInt uses CBOR negative bignum encoding: value = -(n) - 1
    // where n is the unsigned value of the bytes.

    #[test]
    fn bignint_small() {
        // n=4 -> -(4) - 1 = -5
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigNInt(vec![0x04].into()))),
            "-5"
        );
    }

    #[test]
    fn bignint_zero_byte_is_minus_one() {
        // n=0 -> -(0) - 1 = -1
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigNInt(vec![0x00].into()))),
            "-1"
        );
    }

    #[test]
    fn bignint_empty_bytes_is_minus_one() {
        // empty -> n=0 -> -1
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigNInt(vec![].into()))),
            "-1"
        );
    }

    #[test]
    fn bignint_large() {
        // n = 2^64 - 1 -> value = -(2^64 - 1) - 1 = -2^64
        let bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(
            bigint_to_string(&make(big_int::BigInt::BigNInt(bytes.into()))),
            "-18446744073709551616"
        );
    }
}
