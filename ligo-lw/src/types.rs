use crate::error::{Error, Result};

/// LIGO_LW column / param data types.
///
/// Only the types observed in real coinc.xml files emitted by the LVK
/// production pipelines are enumerated. Width-distinguished integer types
/// (int_2s, int_4s, int_8s, ...) are folded into [`LigoType::Int`] because
/// downstream consumers in the alert manager carry these as `i64` regardless;
/// signedness is recorded so the parser can reject overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LigoType {
    /// Signed integer; widths int_2s, int_4s, int_8s are all stored as i64.
    Int,
    /// Unsigned integer; widths int_2u, int_4u, int_8u are all stored as u64.
    UInt,
    /// 32-bit float (real_4) — stored as f64 to keep one numeric path.
    Real4,
    /// 64-bit float (real_8).
    Real8,
    /// Length-delimited string (lstring) or fixed-width string (char_s, char_v).
    Str,
    /// Legacy ilwd:char identifier such as `"process:process_id:0"`. Stored
    /// as an opaque string.
    Ilwd,
}

impl LigoType {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "int_2s" | "int_4s" | "int_8s" => Ok(Self::Int),
            "int_2u" | "int_4u" | "int_8u" => Ok(Self::UInt),
            "real_4" => Ok(Self::Real4),
            "real_8" => Ok(Self::Real8),
            "lstring" | "char_s" | "char_v" | "string" => Ok(Self::Str),
            "ilwd:char" => Ok(Self::Ilwd),
            other => Err(Error::UnknownType(other.to_string())),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::UInt => "uint",
            Self::Real4 => "real_4",
            Self::Real8 => "real_8",
            Self::Str => "string",
            Self::Ilwd => "ilwd:char",
        }
    }

    /// Whether this type is parsed as a quoted literal in stream content.
    pub fn is_quoted(self) -> bool {
        matches!(self, Self::Str | Self::Ilwd)
    }
}
