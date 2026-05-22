//! XML parser for LIGO_LW documents.

use std::borrow::Cow;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use crate::document::{Column, Document, Param, Table};
use crate::error::{Error, Result};
use crate::stream::parse_stream;
use crate::types::LigoType;

/// Parse a complete LIGO_LW document from a byte slice.
pub fn parse_bytes(bytes: &[u8]) -> Result<Document> {
    let mut reader = Reader::from_reader(bytes);
    let config = reader.config_mut();
    config.trim_text(false);
    config.expand_empty_elements = true;

    let mut doc = Document::default();

    // Parser state. Only one Table is ever in progress at a time because
    // LIGO_LW does not nest Tables.
    let mut cur_table: Option<TableInProgress> = None;
    let mut cur_param: Option<ParamInProgress> = None;

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Eof => break,
            Event::Start(ref e) => match e.local_name().as_ref() {
                b"Table" => {
                    let (name, _ty) = read_name_type(e)?;
                    cur_table = Some(TableInProgress::new(strip_table_suffix(&name)));
                }
                b"Column" => {
                    if let Some(t) = cur_table.as_mut() {
                        let (full_name, ty_str) = read_name_type(e)?;
                        let ty = LigoType::parse(&ty_str)?;
                        t.columns.push(Column {
                            name: strip_any_prefix(&full_name),
                            ty,
                        });
                    }
                }
                b"Stream" => {
                    if let Some(t) = cur_table.as_mut() {
                        t.delimiter = read_delimiter(e);
                        t.collecting_stream = true;
                    }
                }
                b"Param" => {
                    let (name, ty_str) = read_name_type(e)?;
                    let ty = LigoType::parse(&ty_str).unwrap_or(LigoType::Str);
                    cur_param = Some(ParamInProgress {
                        name: strip_param_suffix(&name),
                        ty,
                        raw: String::new(),
                    });
                }
                _ => {}
            },
            Event::Text(t) => {
                let text = t.unescape().map_err(quick_xml::Error::from)?;
                if let Some(table) = cur_table.as_mut() {
                    if table.collecting_stream {
                        table.stream_text.push_str(&text);
                    }
                }
                if let Some(p) = cur_param.as_mut() {
                    p.raw.push_str(&text);
                }
            }
            Event::CData(t) => {
                let text = std::str::from_utf8(t.as_ref())?.to_string();
                if let Some(table) = cur_table.as_mut() {
                    if table.collecting_stream {
                        table.stream_text.push_str(&text);
                    }
                }
                if let Some(p) = cur_param.as_mut() {
                    p.raw.push_str(&text);
                }
            }
            Event::End(ref e) => match e.local_name().as_ref() {
                b"Stream" => {
                    if let Some(t) = cur_table.as_mut() {
                        t.collecting_stream = false;
                    }
                }
                b"Table" => {
                    if let Some(t) = cur_table.take() {
                        let table = t.finalize()?;
                        doc.tables.insert(table.name.clone(), table);
                    }
                }
                b"Param" => {
                    if let Some(p) = cur_param.take() {
                        doc.params.insert(
                            p.name.clone(),
                            Param {
                                name: p.name,
                                ty: p.ty,
                                raw: p.raw.trim().to_string(),
                            },
                        );
                    }
                }
                _ => {}
            },
            _ => {}
        }
        buf.clear();
    }

    Ok(doc)
}

struct TableInProgress {
    name: String,
    columns: Vec<Column>,
    delimiter: char,
    stream_text: String,
    collecting_stream: bool,
}

impl TableInProgress {
    fn new(name: String) -> Self {
        Self {
            name,
            columns: Vec::new(),
            delimiter: ',',
            stream_text: String::new(),
            collecting_stream: false,
        }
    }

    fn finalize(self) -> Result<Table> {
        let rows = parse_stream(&self.name, self.delimiter, &self.columns, &self.stream_text)?;
        Ok(Table {
            name: self.name,
            columns: self.columns,
            rows,
        })
    }
}

struct ParamInProgress {
    name: String,
    ty: LigoType,
    raw: String,
}

fn attr<'a>(e: &'a BytesStart, key: &str) -> Result<Option<Cow<'a, str>>> {
    for a in e.attributes() {
        let a = a?;
        let k = std::str::from_utf8(a.key.as_ref())?;
        if k.eq_ignore_ascii_case(key) {
            let v = a.unescape_value().map_err(quick_xml::Error::from)?;
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn read_name_type(e: &BytesStart) -> Result<(String, String)> {
    let name = attr(e, "Name")?
        .ok_or_else(|| Error::MissingAttr {
            element: String::from_utf8_lossy(e.name().as_ref()).into_owned(),
            attr: "Name".into(),
        })?
        .into_owned();
    let ty = attr(e, "Type")?.unwrap_or(Cow::Borrowed("")).into_owned();
    Ok((name, ty))
}

fn read_delimiter(e: &BytesStart) -> char {
    match attr(e, "Delimiter") {
        Ok(Some(s)) if !s.is_empty() => s.chars().next().unwrap_or(','),
        _ => ',',
    }
}

/// `coinc_inspiral:table` -> `coinc_inspiral`.
fn strip_table_suffix(name: &str) -> String {
    if let Some(rest) = name.strip_suffix(":table") {
        rest.to_string()
    } else {
        name.to_string()
    }
}

/// `coinc_inspiral:combined_far` -> `combined_far`. Strips any `<word>:`
/// table prefix from a column name, including cross-table references such
/// as `coinc_event:coinc_event_id` that appear inside `coinc_inspiral`,
/// `coinc_event_map`, and so on. The unprefixed name is what consumers
/// actually want to look the column up by.
fn strip_any_prefix(column_name: &str) -> String {
    column_name
        .split_once(':')
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| column_name.to_string())
}

/// `psd:array` -> `psd` to keep param naming consistent with table naming.
fn strip_param_suffix(name: &str) -> String {
    if let Some((head, _)) = name.rsplit_once(':') {
        head.to_string()
    } else {
        name.to_string()
    }
}
