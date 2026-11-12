//! Postgres `numeric`, held exactly as Postgres holds it: base-10000
//! digits, the weight of the first digit, the sign and the display scale.
//!
//! Equality, ordering and hashing follow Postgres: they compare the value
//! and ignore the scale (`1.5 = 1.50`), `NaN` equals `NaN` and sorts above
//! every other value, and the infinities sort below and above every finite
//! value. The scale is kept, so writing a value back out reproduces the
//! digits Postgres sent.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Postgres's sign word for a positive value.
const SIGN_POS: u16 = 0x0000;
/// Postgres's sign word for a negative value.
const SIGN_NEG: u16 = 0x4000;
/// Postgres's sign word for `NaN`.
const SIGN_NAN: u16 = 0xC000;
/// Postgres's sign word for `Infinity`.
const SIGN_PINF: u16 = 0xD000;
/// Postgres's sign word for `-Infinity`.
const SIGN_NINF: u16 = 0xF000;

/// Decimal digits in one base-10000 digit.
const DEC_DIGITS: usize = 4;
/// The base of a digit.
const BASE: u16 = 10_000;
/// The largest display scale Postgres accepts.
const MAX_SCALE: usize = 16_383;

/// An exact `numeric` value.
#[derive(Debug, Clone)]
pub enum Numeric {
    /// A finite value: `digits[i] * 10000^(weight - i)`, negated when
    /// `negative`. `digits` has no leading or trailing zero digits, and is
    /// empty for zero.
    Finite {
        negative: bool,
        weight: i16,
        scale: u16,
        digits: Box<[u16]>,
    },
    NaN,
    Infinity,
    NegInfinity,
}

/// Why bytes or text are not a `numeric`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericError(pub &'static str);

impl fmt::Display for NumericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not a numeric: {}", self.0)
    }
}

impl std::error::Error for NumericError {}

impl Numeric {
    /// Zero with scale 0.
    pub fn zero() -> Self {
        Numeric::Finite {
            negative: false,
            weight: 0,
            scale: 0,
            digits: Box::new([]),
        }
    }

    /// A finite value from its parts, with leading and trailing zero
    /// digits stripped.
    fn finite(negative: bool, weight: i16, scale: u16, digits: &[u16]) -> Self {
        let leading = digits.iter().take_while(|digit| **digit == 0).count();
        let rest = digits.get(leading..).unwrap_or_default();
        let trailing = rest.iter().rev().take_while(|digit| **digit == 0).count();
        let kept = rest.get(..rest.len() - trailing).unwrap_or_default();
        if kept.is_empty() {
            return Numeric::Finite {
                negative: false,
                weight: 0,
                scale,
                digits: Box::new([]),
            };
        }
        let shift = i16::try_from(leading).unwrap_or(i16::MAX);
        Numeric::Finite {
            negative,
            weight: weight.saturating_sub(shift),
            scale,
            digits: kept.into(),
        }
    }

    /// Decodes Postgres's binary form (`numeric_send`).
    pub fn from_binary(bytes: &[u8]) -> Result<Self, NumericError> {
        let word = |index: usize| -> Result<u16, NumericError> {
            let at = index * 2;
            bytes
                .get(at..at + 2)
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                .ok_or(NumericError("truncated"))
        };
        let count = usize::from(word(0)?);
        let weight = i16::from_be_bytes(word(1)?.to_be_bytes());
        let sign = word(2)?;
        let scale = word(3)?;
        if bytes.len() != 8 + count * 2 {
            return Err(NumericError("length does not match its digit count"));
        }
        match sign {
            SIGN_NAN => return Ok(Numeric::NaN),
            SIGN_PINF => return Ok(Numeric::Infinity),
            SIGN_NINF => return Ok(Numeric::NegInfinity),
            SIGN_POS | SIGN_NEG => {}
            _ => return Err(NumericError("unknown sign")),
        }
        let mut digits = Vec::with_capacity(count);
        for index in 0..count {
            let digit = word(4 + index)?;
            if digit >= BASE {
                return Err(NumericError("digit out of range"));
            }
            digits.push(digit);
        }
        Ok(Numeric::finite(sign == SIGN_NEG, weight, scale, &digits))
    }

    /// Encodes Postgres's binary form (`numeric_recv` reads it).
    pub fn to_binary(&self) -> Vec<u8> {
        let (sign, weight, scale, digits): (u16, i16, u16, &[u16]) = match self {
            Numeric::Finite {
                negative,
                weight,
                scale,
                digits,
            } => (
                if *negative { SIGN_NEG } else { SIGN_POS },
                *weight,
                *scale,
                digits,
            ),
            Numeric::NaN => (SIGN_NAN, 0, 0, &[]),
            Numeric::Infinity => (SIGN_PINF, 0, 0, &[]),
            Numeric::NegInfinity => (SIGN_NINF, 0, 0, &[]),
        };
        let count = u16::try_from(digits.len()).unwrap_or(u16::MAX);
        let mut out = Vec::with_capacity(8 + digits.len() * 2);
        out.extend_from_slice(&count.to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());
        for digit in digits.iter().take(usize::from(count)) {
            out.extend_from_slice(&digit.to_be_bytes());
        }
        out
    }

    /// Parses decimal text as Postgres's `numeric_in` does: an optional
    /// sign, digits with an optional point, an optional exponent, or
    /// `NaN` / `Infinity` / `-Infinity` in any case. The scale is the
    /// number of digits after the point once the exponent is applied.
    pub fn parse(text: &str) -> Result<Self, NumericError> {
        let text = text.trim();
        match text.to_ascii_lowercase().as_str() {
            "nan" => return Ok(Numeric::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => return Ok(Numeric::Infinity),
            "-infinity" | "-inf" => return Ok(Numeric::NegInfinity),
            _ => {}
        }
        let (negative, unsigned) = match text.as_bytes().first() {
            Some(b'-') => (true, text.get(1..).unwrap_or_default()),
            Some(b'+') => (false, text.get(1..).unwrap_or_default()),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(at) => {
                let exponent = unsigned
                    .get(at + 1..)
                    .unwrap_or_default()
                    .parse::<i32>()
                    .map_err(|_| NumericError("bad exponent"))?;
                (unsigned.get(..at).unwrap_or_default(), exponent)
            }
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty() {
            return Err(NumericError("no digits"));
        }
        if !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
        {
            return Err(NumericError("not a decimal number"));
        }
        // Whole numbers may have many more digits than the fractional
        // scale limit. Check the actual resulting weight below.
        const MAX_EXPONENT: i32 = 131_072;
        if !(-MAX_EXPONENT..=MAX_EXPONENT).contains(&exponent) {
            return Err(NumericError("exponent out of range"));
        }
        let digits: Vec<u8> = whole
            .bytes()
            .chain(fraction.bytes())
            .map(|b| b - b'0')
            .collect();
        let point = i64::try_from(whole.len()).unwrap_or(i64::MAX) + i64::from(exponent);
        let fraction_digits = (i64::try_from(digits.len()).unwrap_or(i64::MAX) - point).max(0);
        let scale = u16::try_from(fraction_digits)
            .ok()
            .filter(|scale| usize::from(*scale) <= MAX_SCALE)
            .ok_or(NumericError("scale out of range"))?;
        // Strip leading zeros before deriving the base-10000 weight.
        // Otherwise a long, valid zero prefix can saturate the i16 weight
        // and silently change the value; oversized nonzero values must
        // be rejected rather than saturated too.
        let leading = digits
            .iter()
            .position(|digit| *digit != 0)
            .unwrap_or(digits.len());
        let significant = digits.get(leading..).unwrap_or_default();
        let point = point - i64::try_from(leading).map_err(|_| NumericError("too many digits"))?;
        if !significant.is_empty() && point > 131_072 {
            return Err(NumericError("integer digits out of range"));
        }
        Ok(Self::from_decimal_digits(
            negative,
            significant,
            point,
            scale,
        ))
    }

    /// The value of decimal `digits` with the decimal point after the
    /// first `point` of them (`point` may lie outside the digits).
    fn from_decimal_digits(negative: bool, digits: &[u8], point: i64, scale: u16) -> Self {
        const WIDTH: i64 = 4;
        let width = WIDTH;
        let first_power = point - 1;
        let first_group = first_power.div_euclid(width);
        let lead = width - 1 - first_power.rem_euclid(width);
        let mut groups = Vec::with_capacity(digits.len() / DEC_DIGITS + 2);
        let mut current: u16 = 0;
        let mut filled = usize::try_from(lead).unwrap_or(0);
        for digit in digits {
            current = current * 10 + u16::from(*digit);
            filled += 1;
            if filled == DEC_DIGITS {
                groups.push(current);
                current = 0;
                filled = 0;
            }
        }
        if filled > 0 {
            for _ in filled..DEC_DIGITS {
                current *= 10;
            }
            groups.push(current);
        }
        let weight =
            i16::try_from(first_group).unwrap_or(if first_group < 0 { i16::MIN } else { i16::MAX });
        Numeric::finite(negative, weight, scale, &groups)
    }

    /// The value as the nearest `f64`.
    pub fn to_f64(&self) -> f64 {
        match self {
            Numeric::NaN => f64::NAN,
            Numeric::Infinity => f64::INFINITY,
            Numeric::NegInfinity => f64::NEG_INFINITY,
            Numeric::Finite { .. } => self.to_string().parse().unwrap_or(f64::NAN),
        }
    }

    /// The value as an `i64` when it is a whole number in range.
    pub fn to_i64(&self) -> Option<i64> {
        let Numeric::Finite {
            negative,
            weight,
            digits,
            ..
        } = self
        else {
            return None;
        };
        if digits.is_empty() {
            return Some(0);
        }
        let groups = usize::try_from(*weight).ok()? + 1;
        if digits.len() > groups {
            return None;
        }
        let mut magnitude: i128 = 0;
        for index in 0..groups {
            let digit = digits.get(index).copied().unwrap_or(0);
            magnitude = magnitude
                .checked_mul(i128::from(BASE))?
                .checked_add(i128::from(digit))?;
        }
        let value = if *negative { -magnitude } else { magnitude };
        i64::try_from(value).ok()
    }

    /// The value from an integer, with scale 0.
    pub fn from_i64(value: i64) -> Self {
        let negative = value < 0;
        let mut magnitude = value.unsigned_abs();
        let mut groups = Vec::new();
        while magnitude > 0 {
            groups.push(u16::try_from(magnitude % u64::from(BASE)).unwrap_or(0));
            magnitude /= u64::from(BASE);
        }
        groups.reverse();
        let weight = i16::try_from(groups.len()).unwrap_or(i16::MAX) - 1;
        Numeric::finite(negative, weight, 0, &groups)
    }

    /// Whether the value is a whole number.
    pub fn is_integral(&self) -> bool {
        match self {
            Numeric::Finite { weight, digits, .. } => {
                digits.is_empty() || usize::try_from(*weight).is_ok_and(|w| digits.len() <= w + 1)
            }
            Numeric::NaN | Numeric::Infinity | Numeric::NegInfinity => false,
        }
    }

    /// Rank of the kinds in Postgres's order.
    fn rank(&self) -> u8 {
        match self {
            Numeric::NegInfinity => 0,
            Numeric::Finite { .. } => 1,
            Numeric::Infinity => 2,
            Numeric::NaN => 3,
        }
    }
}

/// Compares two finite magnitudes.
fn cmp_magnitude(a_weight: i16, a: &[u16], b_weight: i16, b: &[u16]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }
    a_weight.cmp(&b_weight).then_with(|| a.cmp(b))
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Numeric::Finite {
                    negative: a_neg,
                    weight: a_weight,
                    digits: a,
                    ..
                },
                Numeric::Finite {
                    negative: b_neg,
                    weight: b_weight,
                    digits: b,
                    ..
                },
            ) => {
                let a_neg = *a_neg && !a.is_empty();
                let b_neg = *b_neg && !b.is_empty();
                match (a_neg, b_neg) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    (false, false) => cmp_magnitude(*a_weight, a, *b_weight, b),
                    (true, true) => cmp_magnitude(*b_weight, b, *a_weight, a),
                }
            }
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Numeric {}

impl Hash for Numeric {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rank().hash(state);
        if let Numeric::Finite {
            negative,
            weight,
            digits,
            ..
        } = self
            && !digits.is_empty()
        {
            negative.hash(state);
            weight.hash(state);
            digits.hash(state);
        }
    }
}

impl fmt::Display for Numeric {
    /// The text `numeric_out` writes: every digit the scale asks for, no
    /// exponent.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (negative, weight, scale, digits) = match self {
            Numeric::NaN => return f.write_str("NaN"),
            Numeric::Infinity => return f.write_str("Infinity"),
            Numeric::NegInfinity => return f.write_str("-Infinity"),
            Numeric::Finite {
                negative,
                weight,
                scale,
                digits,
            } => (*negative, *weight, usize::from(*scale), digits),
        };
        let mut out = String::new();
        if negative && !digits.is_empty() {
            out.push('-');
        }
        if weight < 0 || digits.is_empty() {
            out.push('0');
        } else {
            let whole_groups = usize::try_from(weight).unwrap_or(0) + 1;
            for index in 0..whole_groups {
                let digit = digits.get(index).copied().unwrap_or(0);
                if index == 0 {
                    out.push_str(&digit.to_string());
                } else {
                    out.push_str(&format!("{digit:04}"));
                }
            }
        }
        if scale > 0 {
            out.push('.');
            let mut fraction = String::with_capacity(scale + DEC_DIGITS);
            let mut power = -1i32;
            while fraction.len() < scale {
                let index = i32::from(weight) - power;
                let digit = usize::try_from(index)
                    .ok()
                    .and_then(|index| digits.get(index))
                    .copied()
                    .unwrap_or(0);
                fraction.push_str(&format!("{digit:04}"));
                power -= 1;
            }
            out.push_str(fraction.get(..scale).unwrap_or(&fraction));
        }
        f.write_str(&out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(text: &str) -> Numeric {
        Numeric::parse(text).unwrap()
    }

    #[test]
    fn decimal_weight_never_saturates_input_to_another_value() {
        assert_eq!(
            Numeric::parse(&format!("{}12345", "0".repeat(140_000))).unwrap(),
            Numeric::from_i64(12345)
        );
        assert!(Numeric::parse(&format!("1{}", "0".repeat(131_072))).is_err());
        let large = Numeric::parse("1e20000").unwrap();
        assert_eq!(large.to_string(), format!("1{}", "0".repeat(20_000)));
        assert_eq!(Numeric::from_binary(&large.to_binary()).unwrap(), large);
        assert!(Numeric::parse("1e-16384").is_err());
    }

    #[test]
    fn text_round_trips_with_its_scale() {
        for text in [
            "0",
            "1",
            "-1",
            "1.50",
            "0.0015",
            "12345678.9",
            "10000",
            "0.00",
            "-0.5",
            "99999999999999999999999.000000000000000000001",
            "NaN",
            "Infinity",
            "-Infinity",
        ] {
            assert_eq!(n(text).to_string(), text);
        }
        assert_eq!(n("1e3").to_string(), "1000");
        assert_eq!(n("1.5e-3").to_string(), "0.0015");
        assert_eq!(n("-0").to_string(), "0");
        assert_eq!(n(".5").to_string(), "0.5");
    }

    #[test]
    fn binary_round_trips() {
        for text in [
            "0",
            "1.50",
            "-12345.6789",
            "0.0001",
            "100000000",
            "NaN",
            "-Infinity",
        ] {
            let value = n(text);
            let back = Numeric::from_binary(&value.to_binary()).unwrap();
            assert_eq!(back.to_string(), text);
        }
    }

    /// The bytes `numeric_send` writes for `1.50`: one digit, weight 0,
    /// positive, scale 2, then the digits 1 and 5000.
    #[test]
    fn reads_postgres_bytes() {
        let bytes = [0, 2, 0, 0, 0, 0, 0, 2, 0, 1, 0x13, 0x88];
        assert_eq!(Numeric::from_binary(&bytes).unwrap().to_string(), "1.50");
        assert!(Numeric::from_binary(&bytes[..10]).is_err());
        assert!(Numeric::from_binary(&[0, 0, 0, 0, 0x12, 0x34, 0, 0]).is_err());
    }

    #[test]
    fn equality_ignores_scale() {
        assert_eq!(n("1.5"), n("1.500"));
        assert_eq!(n("0"), n("-0.00"));
        assert_ne!(n("1.5"), n("1.05"));
        let hash = |value: &Numeric| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&n("1.5")), hash(&n("1.500")));
        assert_eq!(hash(&n("0")), hash(&n("-0.0")));
    }

    #[test]
    fn orders_as_postgres_does() {
        let sorted = [
            "-Infinity",
            "-10000",
            "-9999.5",
            "-1",
            "-0.0001",
            "0",
            "0.0001",
            "0.5",
            "1",
            "9999",
            "10000",
            "10000.0001",
            "Infinity",
            "NaN",
        ];
        for pair in sorted.windows(2) {
            assert!(n(pair[0]) < n(pair[1]), "{} < {}", pair[0], pair[1]);
        }
        assert_eq!(n("NaN"), n("nan"));
    }

    #[test]
    fn whole_numbers_convert() {
        assert_eq!(n("12345678").to_i64(), Some(12_345_678));
        assert_eq!(n("-10000.000").to_i64(), Some(-10_000));
        assert_eq!(n("1.5").to_i64(), None);
        assert_eq!(n("9223372036854775808").to_i64(), None);
        assert_eq!(
            Numeric::from_i64(i64::MIN).to_string(),
            i64::MIN.to_string()
        );
        assert_eq!(Numeric::from_i64(0), n("0"));
        assert_eq!(Numeric::from_i64(-20000), n("-20000.00"));
        assert!(n("100.000").is_integral());
        assert!(!n("100.001").is_integral());
    }

    #[test]
    fn rejects_what_is_not_a_number() {
        for text in ["", "-", ".", "1.2.3", "1e", "abc", "1e999999"] {
            assert!(Numeric::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn converts_to_f64() {
        assert_eq!(n("0.1").to_f64(), 0.1);
        assert_eq!(n("-2.5").to_f64(), -2.5);
        assert!(n("NaN").to_f64().is_nan());
    }
}
