use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::types::LigoType;
use crate::value::Value;

/// A parsed LIGO_LW document. Tables are keyed by their bare name with the
/// trailing `:table` suffix stripped (so `coinc_inspiral:table` is stored as
/// `coinc_inspiral`). Params follow the same naming convention.
#[derive(Debug, Default, Clone)]
pub struct Document {
    pub tables: HashMap<String, Table>,
    pub params: HashMap<String, Param>,
}

impl Document {
    /// Borrow a table by its bare name.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Borrow a table or return [`Error::MissingTable`].
    pub fn require_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::MissingTable(name.to_string()))
    }
}

/// A `Table` element from a LIGO_LW document.
#[derive(Debug, Clone)]
pub struct Table {
    /// Bare table name (`coinc_inspiral`, `sngl_inspiral`, ...).
    pub name: String,
    /// Column declarations in document order.
    pub columns: Vec<Column>,
    /// Row data; each row has `columns.len()` cells in document order.
    pub rows: Vec<Vec<Value>>,
}

impl Table {
    /// Look up a column index by bare name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Borrow a cell, returning `None` if either the column does not exist or
    /// the row index is out of range.
    pub fn cell(&self, row: usize, column: &str) -> Option<&Value> {
        let idx = self.column_index(column)?;
        self.rows.get(row)?.get(idx)
    }

    /// Borrow a cell or return [`Error::MissingColumn`].
    pub fn require_cell(&self, row: usize, column: &str) -> Result<&Value> {
        let idx = self.column_index(column).ok_or_else(|| Error::MissingColumn {
            table: self.name.clone(),
            column: column.to_string(),
        })?;
        Ok(&self.rows[row][idx])
    }
}

/// A `Column` declaration inside a `Table`.
#[derive(Debug, Clone)]
pub struct Column {
    /// Bare column name (`end_time`, `snr`, ...). The `table:` prefix used by
    /// some files is stripped during parsing.
    pub name: String,
    pub ty: LigoType,
}

/// A `Param` element. We do not currently parse Param payload data — only the
/// metadata is captured, since the alert-manager pipeline does not depend on
/// it. The raw text content is preserved so callers can postprocess if needed.
#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub ty: LigoType,
    pub raw: String,
}
