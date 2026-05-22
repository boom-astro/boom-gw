use crate::error::{Error, Result};
use crate::types::LigoType;

/// A typed cell value parsed from a LIGO_LW Stream row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Int(i64),
    UInt(u64),
    Real(f64),
    Str(String),
    Null,
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "int",
            Self::UInt(_) => "uint",
            Self::Real(_) => "real",
            Self::Str(_) => "string",
            Self::Null => "null",
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Self::Int(v) => Some(v),
            Self::UInt(v) if v <= i64::MAX as u64 => Some(v as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Self::UInt(v) => Some(v),
            Self::Int(v) if v >= 0 => Some(v as u64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Self::Real(v) => Some(v),
            Self::Int(v) => Some(v as f64),
            Self::UInt(v) => Some(v as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// Parse a single field literal (already trimmed and unquoted as appropriate)
/// into a `Value` using the column's declared type.
pub(crate) fn parse_literal(ty: LigoType, raw: &str, column: &str) -> Result<Value> {
    // The empty literal between two consecutive delimiters represents NULL.
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    match ty {
        LigoType::Int => raw
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|source| Error::BadInt {
                column: column.to_string(),
                literal: raw.to_string(),
                source,
            }),
        LigoType::UInt => raw
            .parse::<u64>()
            .map(Value::UInt)
            .map_err(|source| Error::BadInt {
                column: column.to_string(),
                literal: raw.to_string(),
                source,
            }),
        LigoType::Real4 | LigoType::Real8 => {
            raw.parse::<f64>()
                .map(Value::Real)
                .map_err(|source| Error::BadFloat {
                    column: column.to_string(),
                    literal: raw.to_string(),
                    source,
                })
        }
        LigoType::Str | LigoType::Ilwd => Ok(Value::Str(raw.to_string())),
    }
}
