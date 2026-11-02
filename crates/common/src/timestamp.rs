//! TIMESTAMPTZ values: microseconds since 1970-01-01 00:00:00 UTC.
//!
//! Text is read and written as Postgres does with `DateStyle = ISO` and
//! `TimeZone = UTC`, the settings the server reports to clients, so
//! `2026-11-02 10:51:00.5+00`. Years run from 1 to 9999, a narrower range
//! than Postgres's.

use std::fmt;

use crate::error::{Result, ValueError};

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;

/// 0001-01-01 00:00:00 UTC.
pub const MIN: i64 = days_from_civil(1, 1, 1) * MICROS_PER_DAY;
/// 9999-12-31 23:59:59.999999 UTC.
pub const MAX: i64 = days_from_civil(10_000, 1, 1) * MICROS_PER_DAY - 1;

/// Reads a timestamp: a date `YYYY-MM-DD`, then optionally a time
/// `HH:MM[:SS[.fraction]]` after a space or `T`, then optionally an offset
/// from UTC (`Z`, `UTC`, `+HH`, `+HH:MM` or `+HHMM`). Without an offset the
/// time is in UTC, the session's time zone. Fractions finer than a
/// microsecond are rounded to one.
pub fn parse(text: &str) -> Result<i64> {
    let invalid = || ValueError::InvalidTimestamp(text.to_owned());
    let mut s = Cursor(text.trim().as_bytes());

    let year = s.number(4, 4).ok_or_else(invalid)?;
    s.eat(b'-').then_some(()).ok_or_else(invalid)?;
    let month = s.number(1, 2).ok_or_else(invalid)?;
    s.eat(b'-').then_some(()).ok_or_else(invalid)?;
    let day = s.number(1, 2).ok_or_else(invalid)?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return Err(invalid());
    }

    let mut time = 0;
    if s.eat(b' ') || s.eat(b'T') || s.eat(b't') {
        let hour = s.number(1, 2).ok_or_else(invalid)?;
        s.eat(b':').then_some(()).ok_or_else(invalid)?;
        let minute = s.number(1, 2).ok_or_else(invalid)?;
        let second = if s.eat(b':') {
            s.number(1, 2).ok_or_else(invalid)?
        } else {
            0
        };
        if hour > 23 || minute > 59 || second > 59 {
            return Err(invalid());
        }
        time = ((hour * 60 + minute) * 60 + second) * MICROS_PER_SECOND;
        if s.eat(b'.') {
            time += s.fraction().ok_or_else(invalid)?;
        }
    }

    while s.eat(b' ') {}
    let offset = if s.0.is_empty() || s.eat_ignore_case(b"z") || s.eat_ignore_case(b"utc") {
        0
    } else {
        let sign = if s.eat(b'+') {
            1
        } else if s.eat(b'-') {
            -1
        } else {
            return Err(invalid());
        };
        let hours = s.number(2, 2).ok_or_else(invalid)?;
        s.eat(b':');
        let minutes = if s.0.is_empty() {
            0
        } else {
            s.number(2, 2).ok_or_else(invalid)?
        };
        if hours > 15 || minutes > 59 {
            return Err(invalid());
        }
        sign * (hours * 60 + minutes) * 60 * MICROS_PER_SECOND
    };
    if !s.0.is_empty() {
        return Err(invalid());
    }

    let micros = days_from_civil(year, month, day) * MICROS_PER_DAY + time - offset;
    if !(MIN..=MAX).contains(&micros) {
        return Err(ValueError::TimestampOutOfRange(text.to_owned()));
    }
    Ok(micros)
}

/// Writes `micros` as `YYYY-MM-DD HH:MM:SS[.ffffff]+00`, the fraction
/// without trailing zeros.
pub(crate) fn format(micros: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let (year, month, day) = civil_from_days(micros.div_euclid(MICROS_PER_DAY));
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let seconds = time / MICROS_PER_SECOND;
    write!(
        f,
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )?;
    let mut fraction = time % MICROS_PER_SECOND;
    if fraction != 0 {
        let mut width = 6;
        while fraction % 10 == 0 {
            fraction /= 10;
            width -= 1;
        }
        write!(f, ".{fraction:0width$}")?;
    }
    f.write_str("+00")
}

struct Cursor<'a>(&'a [u8]);

impl Cursor<'_> {
    fn eat(&mut self, byte: u8) -> bool {
        match self.0.split_first() {
            Some((&first, rest)) if first == byte => {
                self.0 = rest;
                true
            }
            _ => false,
        }
    }

    fn eat_ignore_case(&mut self, word: &[u8]) -> bool {
        match self.0.get(..word.len()) {
            Some(prefix) if prefix.eq_ignore_ascii_case(word) => {
                self.0 = &self.0[word.len()..];
                true
            }
            _ => false,
        }
    }

    /// A number of `min` to `max` digits.
    fn number(&mut self, min: usize, max: usize) -> Option<i64> {
        let len = self
            .0
            .iter()
            .take(max)
            .take_while(|b| b.is_ascii_digit())
            .count();
        if len < min {
            return None;
        }
        let (digits, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(digits.iter().fold(0, |n, d| n * 10 + i64::from(d - b'0')))
    }

    /// The digits after a decimal point, in microseconds, rounded half to
    /// even as Postgres does.
    fn fraction(&mut self) -> Option<i64> {
        let len = self.0.iter().take_while(|b| b.is_ascii_digit()).count();
        if len == 0 {
            return None;
        }
        let (digits, rest) = self.0.split_at(len);
        self.0 = rest;
        let mut micros = 0;
        for i in 0..6 {
            micros = micros * 10 + digits.get(i).map_or(0, |d| i64::from(d - b'0'));
        }
        let beyond = &digits[digits.len().min(6)..];
        let round_up = match beyond.split_first() {
            Some((&first, _)) if first > b'5' => true,
            Some((&b'5', rest)) => rest.iter().any(|&d| d != b'0') || micros % 2 == 1,
            _ => false,
        };
        Some(micros + i64::from(round_up))
    }
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days from 1970-01-01 to the date in the proleptic Gregorian calendar
/// (Howard Hinnant's `days_from_civil`).
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The inverse of [`days_from_civil`].
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    fn shown(text: &str) -> String {
        Value::Timestamptz(parse(text).unwrap()).to_string()
    }

    #[test]
    fn reads_what_clients_send() {
        // psycopg, for a Python datetime with a time zone.
        assert_eq!(
            shown("2026-11-02T10:51:00.123456+00:00"),
            "2026-11-02 10:51:00.123456+00"
        );
        assert_eq!(shown("2026-11-02 10:51:00+00"), "2026-11-02 10:51:00+00");
        assert_eq!(shown("2026-11-02"), "2026-11-02 00:00:00+00");
        assert_eq!(shown("2026-11-02 10:51"), "2026-11-02 10:51:00+00");
        assert_eq!(shown(" 2026-1-2 3:04:05Z "), "2026-01-02 03:04:05+00");
        assert_eq!(shown("2026-11-02 10:51:00 UTC"), "2026-11-02 10:51:00+00");
    }

    #[test]
    fn converts_offsets_to_utc() {
        assert_eq!(shown("2026-11-02 10:51:00+05:30"), "2026-11-02 05:21:00+00");
        assert_eq!(shown("2026-11-02 23:00:00-0200"), "2026-11-03 01:00:00+00");
        assert_eq!(shown("2026-03-01 00:30:00+01"), "2026-02-28 23:30:00+00");
    }

    #[test]
    fn shows_fractions_without_trailing_zeros() {
        assert_eq!(shown("2026-11-02 10:51:00.5"), "2026-11-02 10:51:00.5+00");
        assert_eq!(
            shown("2026-11-02 10:51:00.000100"),
            "2026-11-02 10:51:00.0001+00"
        );
        assert_eq!(
            shown("2026-11-02 10:51:00.000000"),
            "2026-11-02 10:51:00+00"
        );
    }

    #[test]
    fn rounds_to_microseconds() {
        assert_eq!(
            shown("2026-11-02 10:51:00.0000005"),
            "2026-11-02 10:51:00+00"
        );
        assert_eq!(
            shown("2026-11-02 10:51:00.0000015"),
            "2026-11-02 10:51:00.000002+00"
        );
        assert_eq!(
            shown("2026-11-02 10:51:00.00000051"),
            "2026-11-02 10:51:00.000001+00"
        );
        assert_eq!(
            shown("2026-12-31 23:59:59.9999999"),
            "2027-01-01 00:00:00+00"
        );
    }

    #[test]
    fn handles_the_calendar() {
        assert_eq!(parse("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse("1969-12-31 23:59:59.999999").unwrap(), -1);
        assert_eq!(shown("2024-02-29"), "2024-02-29 00:00:00+00");
        assert_eq!(shown("2000-02-29"), "2000-02-29 00:00:00+00");
        assert!(parse("2023-02-29").is_err());
        assert!(parse("1900-02-29").is_err());
        assert_eq!(shown("0001-01-01"), "0001-01-01 00:00:00+00");
        assert_eq!(
            shown("9999-12-31 23:59:59.999999"),
            "9999-12-31 23:59:59.999999+00"
        );
    }

    #[test]
    fn rejects_malformed_text() {
        for text in [
            "",
            "garbage",
            "2026-13-01",
            "2026-00-10",
            "2026-11-31",
            "26-11-02",
            "2026-11-02 24:00:00",
            "2026-11-02 10:60:00",
            "2026-11-02 10:51:00.",
            "2026-11-02 10:51:00+16",
            "2026-11-02 10:51:00 Europe/Paris",
            "2026-11-02x",
        ] {
            assert_eq!(
                parse(text),
                Err(ValueError::InvalidTimestamp(text.to_owned())),
                "{text:?}"
            );
        }
    }

    #[test]
    fn rejects_instants_outside_years_1_to_9999() {
        assert!(matches!(
            parse("0001-01-01 00:00:00+01"),
            Err(ValueError::TimestampOutOfRange(_))
        ));
        assert!(matches!(
            parse("9999-12-31 23:30:00-01"),
            Err(ValueError::TimestampOutOfRange(_))
        ));
    }
}
