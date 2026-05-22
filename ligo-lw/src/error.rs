use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("XML parse error: {0}")]
    Xml(#[from] quick_xml::Error),

    #[error("XML attribute error: {0}")]
    Attr(#[from] quick_xml::events::attributes::AttrError),

    #[error("UTF-8 error in element text: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("invalid integer literal {literal:?} for column {column}: {source}")]
    BadInt {
        column: String,
        literal: String,
        #[source]
        source: std::num::ParseIntError,
    },

    #[error("invalid float literal {literal:?} for column {column}: {source}")]
    BadFloat {
        column: String,
        literal: String,
        #[source]
        source: std::num::ParseFloatError,
    },

    #[error("stream row in table {table:?} has {got} fields but {expected} columns are declared")]
    RowWidthMismatch {
        table: String,
        got: usize,
        expected: usize,
    },

    #[error("unterminated quoted string in stream for table {0:?}")]
    UnterminatedString(String),

    #[error("unknown LIGO_LW type {0:?}")]
    UnknownType(String),

    #[error("missing required attribute {attr:?} on element {element:?}")]
    MissingAttr { element: String, attr: String },

    #[error("LIGO_LW document does not contain table {0:?}")]
    MissingTable(String),

    #[error("LIGO_LW table {table:?} does not contain column {column:?}")]
    MissingColumn { table: String, column: String },

    #[error("value in {table:?}.{column:?} has type {actual} but {expected} was expected")]
    TypeMismatch {
        table: String,
        column: String,
        expected: &'static str,
        actual: &'static str,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
