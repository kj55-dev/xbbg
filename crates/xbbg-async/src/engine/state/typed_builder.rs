//! Typed Arrow array builders for dynamic schema support.
//!
//! This module provides:
//! - `TypedBuilder`: A builder that can hold different Arrow array builder types
//! - `ColumnSet`: A collection of named columns for building RecordBatches
//!
//! These work directly with `xbbg_core::Value` - no JSON intermediate.

use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use xbbg_core::{BlpError, Value};

use super::value_utils::value_to_string;

const MICROS_PER_DAY: i64 = 86_400_000_000;

macro_rules! append_converted_value {
    ($builder:expr, $value:expr, $convert:expr) => {{
        if let Some(v) = $value.and_then($convert) {
            $builder.append_value(v);
        } else {
            $builder.append_null();
        }
    }};
}

/// Arrow type identifier (subset of Arrow types we support).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArrowType {
    Float64,
    Int64,
    Int32,
    String,
    Bool,
    Date32,
    TimestampMicros,
    Time64Micros,
}

impl ArrowType {
    /// Parse from type string (e.g., "float64", "int64", "string").
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "float64" | "float" | "double" | "f64" => ArrowType::Float64,
            "int64" | "int" | "integer" | "i64" => ArrowType::Int64,
            "int32" | "i32" => ArrowType::Int32,
            "bool" | "boolean" => ArrowType::Bool,
            "date32" | "date" => ArrowType::Date32,
            "timestamp" | "datetime" | "timestamp_us" => ArrowType::TimestampMicros,
            "time64" | "time" | "time64_us" => ArrowType::Time64Micros,
            _ => ArrowType::String, // Default to string
        }
    }

    /// Infer ArrowType from a xbbg_core::Value.
    pub fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Null => ArrowType::String, // Default null to string
            Value::Bool(_) => ArrowType::Bool,
            Value::Int32(_) => ArrowType::Int32,
            Value::Int64(_) => ArrowType::Int64,
            Value::Float64(_) => ArrowType::Float64,
            Value::String(_) | Value::Enum(_) => ArrowType::String,
            Value::Date32(_) => ArrowType::Date32,
            Value::TimestampMicros(_) | Value::Datetime(_) => ArrowType::TimestampMicros,
            Value::Time64Micros(_) => ArrowType::Time64Micros,
            Value::Byte(_) => ArrowType::Int32, // Promote byte to int32
        }
    }

    /// Get the Arrow DataType for this type.
    pub fn to_arrow_datatype(&self) -> DataType {
        match self {
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::String => DataType::Utf8,
            ArrowType::Bool => DataType::Boolean,
            ArrowType::Date32 => DataType::Date32,
            ArrowType::TimestampMicros => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            ArrowType::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
        }
    }

    /// Get type name string.
    pub fn type_name(&self) -> &'static str {
        match self {
            ArrowType::Float64 => "float64",
            ArrowType::Int64 => "int64",
            ArrowType::Int32 => "int32",
            ArrowType::String => "string",
            ArrowType::Bool => "bool",
            ArrowType::Date32 => "date32",
            ArrowType::TimestampMicros => "timestamp",
            ArrowType::Time64Micros => "time64",
        }
    }
}

/// A builder that can hold different Arrow array builder types.
pub enum TypedBuilder {
    Float64(Float64Builder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    String(StringBuilder),
    Bool(BooleanBuilder),
    Date32(Date32Builder),
    TimestampMicros(TimestampMicrosecondBuilder),
    Time64Micros(Time64MicrosecondBuilder),
}

impl TypedBuilder {
    fn as_array_builder(&self) -> &dyn ArrayBuilder {
        match self {
            TypedBuilder::Float64(b) => b,
            TypedBuilder::Int64(b) => b,
            TypedBuilder::Int32(b) => b,
            TypedBuilder::String(b) => b,
            TypedBuilder::Bool(b) => b,
            TypedBuilder::Date32(b) => b,
            TypedBuilder::TimestampMicros(b) => b,
            TypedBuilder::Time64Micros(b) => b,
        }
    }

    fn as_array_builder_mut(&mut self) -> &mut dyn ArrayBuilder {
        match self {
            TypedBuilder::Float64(b) => b,
            TypedBuilder::Int64(b) => b,
            TypedBuilder::Int32(b) => b,
            TypedBuilder::String(b) => b,
            TypedBuilder::Bool(b) => b,
            TypedBuilder::Date32(b) => b,
            TypedBuilder::TimestampMicros(b) => b,
            TypedBuilder::Time64Micros(b) => b,
        }
    }

    /// Create a new builder from an ArrowType.
    pub fn new(arrow_type: ArrowType) -> Self {
        match arrow_type {
            ArrowType::Float64 => TypedBuilder::Float64(Float64Builder::new()),
            ArrowType::Int64 => TypedBuilder::Int64(Int64Builder::new()),
            ArrowType::Int32 => TypedBuilder::Int32(Int32Builder::new()),
            ArrowType::String => TypedBuilder::String(StringBuilder::new()),
            ArrowType::Bool => TypedBuilder::Bool(BooleanBuilder::new()),
            ArrowType::Date32 => TypedBuilder::Date32(Date32Builder::new()),
            ArrowType::TimestampMicros => TypedBuilder::TimestampMicros(
                TimestampMicrosecondBuilder::new().with_timezone("UTC"),
            ),
            ArrowType::Time64Micros => TypedBuilder::Time64Micros(Time64MicrosecondBuilder::new()),
        }
    }

    /// Create a new builder from a type string.
    pub fn from_type_str(type_str: &str) -> Self {
        Self::new(ArrowType::parse(type_str))
    }

    /// Append a value from xbbg_core::Value, converting as needed.
    pub fn append_value(&mut self, value: Option<Value<'_>>) {
        match self {
            TypedBuilder::Float64(b) => append_converted_value!(b, value, |v| v.as_f64()),
            TypedBuilder::Int64(b) => append_converted_value!(b, value, |v| v.as_i64()),
            TypedBuilder::Int32(b) => append_converted_value!(b, value, value_to_i32),
            TypedBuilder::String(b) => match value {
                Some(value) => append_string_value(b, value),
                None => b.append_null(),
            },
            TypedBuilder::Bool(b) => append_converted_value!(b, value, |v| v.as_bool()),
            TypedBuilder::Date32(b) => append_converted_value!(b, value, value_to_date32),
            TypedBuilder::TimestampMicros(b) => {
                append_converted_value!(b, value, value_to_timestamp_micros)
            }
            TypedBuilder::Time64Micros(b) => {
                append_converted_value!(b, value, value_to_time64_micros)
            }
        }
    }

    /// Append a string value directly.
    pub fn append_str(&mut self, s: &str) {
        match self {
            TypedBuilder::String(b) => b.append_value(s),
            _ => self.append_value(Some(Value::String(s))),
        }
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        match self {
            TypedBuilder::Float64(b) => b.append_null(),
            TypedBuilder::Int64(b) => b.append_null(),
            TypedBuilder::Int32(b) => b.append_null(),
            TypedBuilder::String(b) => b.append_null(),
            TypedBuilder::Bool(b) => b.append_null(),
            TypedBuilder::Date32(b) => b.append_null(),
            TypedBuilder::TimestampMicros(b) => b.append_null(),
            TypedBuilder::Time64Micros(b) => b.append_null(),
        }
    }

    /// Get the number of values appended.
    pub fn len(&self) -> usize {
        self.as_array_builder().len()
    }

    /// Check if builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish building and return the array.
    pub fn finish(&mut self) -> ArrayRef {
        self.as_array_builder_mut().finish()
    }

    /// Get the Arrow DataType for this builder.
    pub fn data_type(&self) -> DataType {
        self.arrow_type().to_arrow_datatype()
    }

    /// Get the ArrowType for this builder.
    pub fn arrow_type(&self) -> ArrowType {
        match self {
            TypedBuilder::Float64(_) => ArrowType::Float64,
            TypedBuilder::Int64(_) => ArrowType::Int64,
            TypedBuilder::Int32(_) => ArrowType::Int32,
            TypedBuilder::String(_) => ArrowType::String,
            TypedBuilder::Bool(_) => ArrowType::Bool,
            TypedBuilder::Date32(_) => ArrowType::Date32,
            TypedBuilder::TimestampMicros(_) => ArrowType::TimestampMicros,
            TypedBuilder::Time64Micros(_) => ArrowType::Time64Micros,
        }
    }
}

/// A collection of named columns for building RecordBatches.
///
/// Handles dynamic column creation and ensures all columns have the same length.
///
/// # Example
///
/// ```ignore
/// let mut cols = ColumnSet::new();
/// cols.append("ticker", Value::String("AAPL US Equity"));
/// cols.append("price", Value::Float64(150.0));
/// cols.end_row(); // Ensures all columns have same length
///
/// let batch = cols.finish()?;
/// ```
pub struct ColumnSet {
    /// Columns in insertion order (preserves field order)
    columns: IndexMap<String, TypedBuilder>,
    /// Type hints for columns (optional, from field_types config)
    type_hints: IndexMap<String, ArrowType>,
    /// Current row count
    row_count: usize,
}

impl ColumnSet {
    fn default_order_type(name: &str) -> ArrowType {
        match name {
            "value_f64" => ArrowType::Float64,
            "value_i64" => ArrowType::Int64,
            "value_bool" => ArrowType::Bool,
            "value_date" => ArrowType::Date32,
            "value_ts" => ArrowType::TimestampMicros,
            _ => ArrowType::String,
        }
    }

    fn new_builder_for_column(&self, name: &str, value: Option<&Value<'_>>) -> TypedBuilder {
        let arrow_type = self
            .type_hints
            .get(name)
            .copied()
            .unwrap_or_else(|| value.map_or(ArrowType::String, ArrowType::from_value));
        TypedBuilder::new(arrow_type)
    }

    fn build_empty_with_order(&self, order: &[&str]) -> Result<RecordBatch, BlpError> {
        if order.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let mut fields = Vec::with_capacity(order.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(order.len());

        for &name in order {
            let arrow_type = self
                .type_hints
                .get(name)
                .copied()
                .unwrap_or_else(|| Self::default_order_type(name));
            fields.push(Field::new(name, arrow_type.to_arrow_datatype(), true));
            arrays.push(TypedBuilder::new(arrow_type).finish());
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build empty RecordBatch from order: {e}"),
        })
    }

    /// Create a new empty ColumnSet.
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
            type_hints: IndexMap::new(),
            row_count: 0,
        }
    }

    /// Create with type hints for specific columns.
    pub fn with_type_hints(hints: impl IntoIterator<Item = (String, ArrowType)>) -> Self {
        Self {
            columns: IndexMap::new(),
            type_hints: hints.into_iter().collect(),
            row_count: 0,
        }
    }

    /// Set a type hint for a column.
    pub fn set_type_hint(&mut self, name: &str, arrow_type: ArrowType) {
        self.type_hints.insert(name.to_string(), arrow_type);
    }

    /// Append a value to a column.
    ///
    /// Creates the column if it doesn't exist, inferring type from the value
    /// or using type hints if available.
    pub fn append(&mut self, name: &str, value: Value<'_>) {
        let builder = self.new_builder_for_column(name, Some(&value));
        let builder = self.columns.entry(name.to_string()).or_insert(builder);
        builder.append_value(Some(value));
    }

    /// Append a string value to a column (convenience method).
    pub fn append_str(&mut self, name: &str, value: &str) {
        self.append(name, Value::String(value));
    }

    /// Append a null to a column.
    pub fn append_null(&mut self, name: &str) {
        if let Some(builder) = self.columns.get_mut(name) {
            builder.append_null();
        } else {
            let mut builder = self.new_builder_for_column(name, None);
            builder.append_null();
            self.columns.insert(name.to_string(), builder);
        }
    }

    /// End the current row, ensuring all columns have the same length.
    ///
    /// Call this after appending all values for a row. Any columns that
    /// weren't updated will get a null appended.
    pub fn end_row(&mut self) {
        self.row_count += 1;
        for builder in self.columns.values_mut() {
            while builder.len() < self.row_count {
                builder.append_null();
            }
        }
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    /// Get column names in order.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|s| s.as_str())
    }

    /// Finish building and return a RecordBatch.
    pub fn finish(self) -> Result<RecordBatch, BlpError> {
        if self.columns.is_empty() {
            // Return empty batch with no columns
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        // Build schema and arrays
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());

        for (name, mut builder) in self.columns {
            fields.push(Field::new(&name, builder.data_type(), true));
            arrays.push(builder.finish());
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }

    /// Build with a specific column order.
    ///
    /// Columns not in `order` are appended at the end.
    /// Columns in `order` but not in the set are skipped.
    pub fn finish_with_order(mut self, order: &[&str]) -> Result<RecordBatch, BlpError> {
        if self.columns.is_empty() {
            return self.build_empty_with_order(order);
        }

        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        let mut used = std::collections::HashSet::new();

        // First, add columns in specified order
        for &name in order {
            if let Some(mut builder) = self.columns.swap_remove(name) {
                fields.push(Field::new(name, builder.data_type(), true));
                arrays.push(builder.finish());
                used.insert(name.to_string());
            }
        }

        // Then, add remaining columns in their original order
        for (name, mut builder) in self.columns {
            if !used.contains(&name) {
                fields.push(Field::new(&name, builder.data_type(), true));
                arrays.push(builder.finish());
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }
}

fn append_string_value(builder: &mut StringBuilder, value: Value<'_>) {
    match value {
        Value::Null => builder.append_null(),
        Value::String(s) | Value::Enum(s) => builder.append_value(s),
        value => {
            let s = value_to_string(&value);
            builder.append_value(s.as_ref());
        }
    }
}

fn value_to_i32(value: Value<'_>) -> Option<i32> {
    match value {
        Value::Int32(i) => Some(i),
        Value::Int64(i) => Some(i as i32),
        Value::Byte(i) => Some(i as i32),
        Value::Bool(b) => Some(if b { 1 } else { 0 }),
        _ => None,
    }
}

fn value_to_date32(value: Value<'_>) -> Option<i32> {
    match value {
        Value::Date32(d) => Some(d),
        Value::TimestampMicros(ts) => Some((ts / MICROS_PER_DAY) as i32),
        _ => None,
    }
}

fn value_to_timestamp_micros(value: Value<'_>) -> Option<i64> {
    match value {
        Value::TimestampMicros(ts) => Some(ts),
        Value::Datetime(dt) => Some(dt.to_micros()),
        Value::Date32(d) => Some(d as i64 * MICROS_PER_DAY),
        _ => None,
    }
}

fn value_to_time64_micros(value: Value<'_>) -> Option<i64> {
    match value {
        Value::Time64Micros(ts) => Some(ts),
        Value::TimestampMicros(ts) => {
            // Extract time-of-day from full timestamp
            Some(ts.rem_euclid(MICROS_PER_DAY))
        }
        _ => None,
    }
}

impl Default for ColumnSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Create an Arrow Field from a field name and ArrowType.
pub fn create_field(name: &str, arrow_type: ArrowType, nullable: bool) -> Field {
    Field::new(name, arrow_type.to_arrow_datatype(), nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray, TimestampMicrosecondArray};

    #[test]
    fn test_arrow_type_parse() {
        assert_eq!(ArrowType::parse("float64"), ArrowType::Float64);
        assert_eq!(ArrowType::parse("INT64"), ArrowType::Int64);
        assert_eq!(ArrowType::parse("string"), ArrowType::String);
        assert_eq!(ArrowType::parse("unknown"), ArrowType::String);
    }

    #[test]
    fn test_arrow_type_from_value() {
        assert_eq!(
            ArrowType::from_value(&Value::Float64(1.0)),
            ArrowType::Float64
        );
        assert_eq!(ArrowType::from_value(&Value::Int64(1)), ArrowType::Int64);
        assert_eq!(
            ArrowType::from_value(&Value::String("x")),
            ArrowType::String
        );
        assert_eq!(ArrowType::from_value(&Value::Bool(true)), ArrowType::Bool);
    }

    #[test]
    fn test_typed_builder_int32_coercions() {
        let mut builder = TypedBuilder::new(ArrowType::Int32);

        builder.append_value(Some(Value::Bool(true)));
        builder.append_value(Some(Value::Byte(7)));
        builder.append_value(None);

        let array = builder.finish();
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 7);
        assert!(array.is_null(2));
    }

    #[test]
    fn test_typed_builder_string_formats_non_string_values() {
        let mut builder = TypedBuilder::new(ArrowType::String);

        builder.append_value(Some(Value::Bool(true)));
        builder.append_value(Some(Value::Date32(0)));
        builder.append_value(Some(Value::Null));

        let array = builder.finish();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "true");
        assert_eq!(array.value(1), "1970-01-01");
        assert!(array.is_null(2));
    }

    #[test]
    fn test_typed_builder_timestamp_micros_uses_utc_timezone() {
        let mut builder = TypedBuilder::new(ArrowType::TimestampMicros);

        builder.append_value(Some(Value::TimestampMicros(0)));

        let array = builder.finish();
        let array = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(array.timezone(), Some("UTC"));
        assert_eq!(array.value(0), 0);
    }

    #[test]
    fn test_column_set_basic() {
        let mut cols = ColumnSet::new();

        cols.append("ticker", Value::String("AAPL"));
        cols.append("price", Value::Float64(150.0));
        cols.end_row();

        cols.append("ticker", Value::String("MSFT"));
        cols.append("price", Value::Float64(300.0));
        cols.end_row();

        assert_eq!(cols.row_count(), 2);
        assert_eq!(cols.column_count(), 2);

        let batch = cols.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_column_set_with_nulls() {
        let mut cols = ColumnSet::new();

        cols.append("a", Value::Int64(1));
        cols.append("b", Value::Int64(2));
        cols.end_row();

        cols.append("a", Value::Int64(3));
        // Don't append "b" - should get null
        cols.end_row();

        let batch = cols.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_column_set_append_null_creates_string_column() {
        let mut cols = ColumnSet::new();

        cols.append_null("missing");
        cols.end_row();

        let batch = cols.finish().unwrap();
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(batch.schema().field(0).name(), "missing");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn test_column_set_type_hints() {
        let mut cols = ColumnSet::with_type_hints([("price".to_string(), ArrowType::Float64)]);

        // First value is null, but we want float64 column
        cols.append_null("price");
        cols.end_row();

        cols.append("price", Value::Float64(100.0));
        cols.end_row();

        let batch = cols.finish().unwrap();
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_finish_with_order_empty_columns_non_matching_hints() {
        let cols = ColumnSet::with_type_hints([("PX_LAST".to_string(), ArrowType::Float64)]);
        let batch = cols
            .finish_with_order(&["ticker", "field", "value"])
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "field");
        assert_eq!(batch.schema().field(2).name(), "value");
    }

    #[test]
    fn test_finish_with_order_empty_columns_no_hints() {
        let cols = ColumnSet::new();
        let batch = cols
            .finish_with_order(&["ticker", "field", "value"])
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "field");
        assert_eq!(batch.schema().field(2).name(), "value");
    }
}
