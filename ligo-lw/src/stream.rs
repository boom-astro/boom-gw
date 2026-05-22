//! Parser for the body text of a `<Stream>` element.
//!
//! The LIGO_LW Stream format is a delimiter-separated sequence of values whose
//! row boundaries are implicit: after every N values (where N is the table's
//! column count), a new row begins. Strings are wrapped in double quotes, and
//! embedded quotes are doubled (CSV convention). Whitespace and newlines
//! outside quoted strings are ignored.

use crate::document::Column;
use crate::error::{Error, Result};
use crate::value::{parse_literal, Value};

/// Parse the body of a Stream element into rows. The delimiter is typically
/// `,` but is provided by the caller because some tables override it via the
/// `Delimiter` attribute.
pub(crate) fn parse_stream(
    table_name: &str,
    delimiter: char,
    columns: &[Column],
    body: &str,
) -> Result<Vec<Vec<Value>>> {
    if columns.is_empty() {
        // An empty column set means the stream cannot be parsed into rows.
        // Tolerate an empty body; reject a non-empty body.
        if body.trim().is_empty() {
            return Ok(Vec::new());
        }
        return Err(Error::RowWidthMismatch {
            table: table_name.to_string(),
            got: 1,
            expected: 0,
        });
    }

    let mut fields: Vec<Value> = Vec::new();
    let mut col_idx = 0usize;

    let mut buf = String::new();
    let mut in_quotes = false;
    let mut field_started = false;
    // Set once a closing quote has been seen for the current field. Once set,
    // the only meaningful characters until the next delimiter are whitespace,
    // which is silently ignored — the contents of the quoted string are final.
    let mut quote_closed = false;

    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                // A doubled `""` inside a quoted string is a literal `"`. Otherwise,
                // the quote closes the string.
                if chars.peek() == Some(&'"') {
                    chars.next();
                    buf.push('"');
                } else {
                    in_quotes = false;
                    quote_closed = true;
                }
            } else {
                buf.push(c);
            }
            continue;
        }

        if c == '"' {
            in_quotes = true;
            field_started = true;
            continue;
        }

        if c == delimiter {
            // Close out the current field. An entirely empty buffer with no
            // field_started flag means NULL between two delimiters.
            let column = &columns[col_idx];
            let literal = buf.trim();
            let value = if !field_started && literal.is_empty() {
                Value::Null
            } else if column.ty.is_quoted() {
                // Strings: take the buffer verbatim (already unquoted).
                Value::Str(buf.clone())
            } else {
                parse_literal(column.ty, literal, &column.name)?
            };
            fields.push(value);
            buf.clear();
            field_started = false;
            quote_closed = false;
            col_idx = (col_idx + 1) % columns.len();
            continue;
        }

        if c.is_whitespace() {
            // Whitespace between fields is ignored when the field is not yet
            // started; otherwise it is preserved inside the buffer for the
            // literal parser to trim.
            if field_started && !quote_closed {
                buf.push(c);
            }
            continue;
        }

        if quote_closed {
            // Non-whitespace after a closing quote and before the next
            // delimiter is unexpected; ignore it for tolerance rather than
            // failing the whole document.
            continue;
        }

        field_started = true;
        buf.push(c);
    }

    if in_quotes {
        return Err(Error::UnterminatedString(table_name.to_string()));
    }

    // If the stream ended mid-field (i.e. the last character was not a
    // delimiter), emit a final value.
    if field_started || !buf.is_empty() {
        let column = &columns[col_idx];
        let literal = buf.trim();
        let value = if column.ty.is_quoted() {
            Value::Str(buf.clone())
        } else {
            parse_literal(column.ty, literal, &column.name)?
        };
        fields.push(value);
    }

    // Partition the flat field list into rows. A trailing complete row of
    // zero fields (e.g. caused by `1,2,3,` at the end of a 3-column stream)
    // is silently dropped.
    if fields.len() % columns.len() != 0 {
        return Err(Error::RowWidthMismatch {
            table: table_name.to_string(),
            got: fields.len() % columns.len(),
            expected: columns.len(),
        });
    }
    let mut rows = Vec::with_capacity(fields.len() / columns.len());
    let mut iter = fields.into_iter();
    loop {
        let row: Vec<Value> = (&mut iter).take(columns.len()).collect();
        if row.is_empty() {
            break;
        }
        rows.push(row);
    }
    Ok(rows)
}
