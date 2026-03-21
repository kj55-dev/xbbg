//! BQL (Bloomberg Query Language) state with Arrow builders.
//!
//! BQL responses contain structured result data that we extract directly
//! from Bloomberg Elements without JSON intermediate serialization.
//!
//! Note: BQL can return complex nested structures. We flatten them into
//! a tabular format with id column + value columns per field.

use arrow::array::{ArrayRef, Float64Builder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::oneshot;

use super::typed_builder::ColumnSet;
use xbbg_core::{BlpError, Message};

/// State for a BQL request.
pub struct BqlState {
    /// Column set for building the output
    columns: ColumnSet,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    /// Accumulated JSON string (for JSON-encoded responses)
    json_buffer: Option<String>,
}

impl BqlState {
    /// Create a new BQL state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            columns: ColumnSet::new(),
            reply,
            json_buffer: None,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        self.process_message(msg);

        // If we accumulated JSON, try to parse it
        let result = if let Some(json_str) = self.json_buffer.take() {
            self.parse_bql_json(&json_str)
        } else {
            self.columns.finish()
        };

        if let Ok(ref batch) = result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "bql finish"
            );
        }
        let _ = self.reply.send(result);
    }

    /// Process a BQL response message using Element API.
    ///
    /// BQL response structure:
    /// ```text
    /// beqlData {
    ///   results[] {
    ///     ... varies by query
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Try different BQL response structures
        // Structure 1: beqlData -> results
        if let Some(beql_data) = root.get_by_str("beqlData") {
            if let Some(results) = beql_data.get_by_str("results") {
                if self.store_json_buffer_from_result(&results) {
                    return;
                }
                self.extract_results(&results);
                return;
            }

            if self.store_json_buffer(beql_data.get_value(0)) {
                return;
            }
        }

        // Structure 2: Direct results array
        if let Some(results) = root.get_by_str("results") {
            self.extract_results(&results);
            return;
        }

        // Structure 3: Check if root contains a JSON string value
        if self.store_json_buffer(root.get_value(0)) {
            return;
        }

        // Structure 4: Flatten the entire response (fallback)
        self.flatten_element("", &root);
    }

    /// Parse BQL JSON response into a proper table.
    ///
    /// BQL JSON structure:
    /// ```json
    /// {
    ///   "results": {
    ///     "field_name": {
    ///       "idColumn": { "values": ["ticker1", "ticker2", ...] },
    ///       "valuesColumn": { "values": [value1, value2, ...] }
    ///     },
    ///     ...
    ///   }
    /// }
    /// ```
    fn parse_bql_json(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        let json: JsonValue = serde_json::from_str(json_str).map_err(|e| BlpError::Internal {
            detail: format!("Failed to parse BQL JSON: {}", e),
        })?;

        let results = json.get("results").ok_or_else(|| BlpError::Internal {
            detail: "BQL JSON missing 'results' field".into(),
        })?;

        let results_obj = results.as_object().ok_or_else(|| BlpError::Internal {
            detail: "BQL 'results' is not an object".into(),
        })?;

        if results_obj.is_empty() {
            // Return empty batch
            let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, true)]);
            return RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
            )
            .map_err(|e| BlpError::Internal {
                detail: format!("Failed to create empty batch: {}", e),
            });
        }

        // Collect field names and determine row count from first field
        let field_names: Vec<&str> = results_obj.keys().map(String::as_str).collect();
        let mut id_values: Vec<String> = Vec::new();
        let mut field_columns: Vec<(&str, Vec<Option<JsonValue>>)> = Vec::new();

        for &field_name in &field_names {
            let field_data = &results_obj[field_name];

            // Extract idColumn values (only need to do this once)
            if id_values.is_empty() {
                id_values = Self::json_values(field_data, "idColumn")
                    .map(|values| values.iter().map(Self::json_id_value).collect())
                    .unwrap_or_default();
            }

            // Extract valuesColumn values
            let mut values: Vec<Option<JsonValue>> = Self::json_values(field_data, "valuesColumn")
                .map(|vals| {
                    vals.iter()
                        .map(|value| {
                            if value.is_null() {
                                None
                            } else {
                                Some(value.clone())
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Pad values to match id_values length if needed
            while values.len() < id_values.len() {
                values.push(None);
            }

            field_columns.push((field_name, values));
        }

        // Build Arrow arrays
        // Use "ticker" for the id column to avoid conflicts with user-requested "id" field
        let mut id_builder = StringBuilder::new();
        for v in &id_values {
            id_builder.append_value(v);
        }

        let mut fields = vec![Field::new("ticker", DataType::Utf8, true)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(id_builder.finish())];

        // Value columns - detect type from first non-null value
        for (name, values) in &field_columns {
            // Detect if numeric
            let is_numeric = values
                .iter()
                .any(|v| matches!(v, Some(JsonValue::Number(_))));

            if is_numeric {
                let mut builder = Float64Builder::new();
                for v in values {
                    match v {
                        Some(JsonValue::Number(n)) => {
                            builder.append_value(n.as_f64().unwrap_or(f64::NAN));
                        }
                        Some(JsonValue::String(s)) => {
                            // Try to parse string as number
                            if let Ok(f) = s.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(*name, DataType::Float64, true));
                arrays.push(Arc::new(builder.finish()));
            } else {
                let mut builder = StringBuilder::new();
                for v in values {
                    match v {
                        Some(JsonValue::String(s)) => builder.append_value(s),
                        Some(JsonValue::Null) | None => builder.append_null(),
                        Some(other) => builder.append_value(other.to_string()),
                    }
                }
                fields.push(Field::new(*name, DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()));
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("Failed to create RecordBatch: {}", e),
        })
    }

    /// Extract results from a BQL results element.
    fn extract_results(&mut self, results: &xbbg_core::Element) {
        let n = results.len();
        for i in 0..n {
            if let Some(row) = results.get_element(i) {
                // Each result row - extract all fields
                let num_children = row.num_children();
                for j in 0..num_children {
                    if let Some(child) = row.get_at(j) {
                        let name = child.name();
                        let name_str = name.as_str();
                        if let Some(value) = child.get_value(0) {
                            self.columns.append(name_str, value);
                        } else {
                            self.columns.append_null(name_str);
                        }
                    }
                }
                self.columns.end_row();
            }
        }
    }

    fn store_json_buffer(&mut self, value: Option<xbbg_core::Value>) -> bool {
        if let Some(json) = Self::json_string(value) {
            self.json_buffer = Some(json);
            true
        } else {
            false
        }
    }

    fn store_json_buffer_from_result(&mut self, results: &xbbg_core::Element) -> bool {
        results
            .get_element(0)
            .and_then(|first| first.get_value(0))
            .is_some_and(|value| self.store_json_buffer(Some(value)))
    }

    fn json_string(value: Option<xbbg_core::Value>) -> Option<String> {
        match value {
            Some(xbbg_core::Value::String(s)) if s.starts_with('{') => Some(s.to_string()),
            _ => None,
        }
    }

    fn json_values<'a>(field_data: &'a JsonValue, key: &str) -> Option<&'a [JsonValue]> {
        field_data
            .get(key)?
            .get("values")?
            .as_array()
            .map(Vec::as_slice)
    }

    fn json_id_value(value: &JsonValue) -> String {
        match value {
            JsonValue::String(s) => s.clone(),
            JsonValue::Null => String::new(),
            other => other.to_string(),
        }
    }

    /// Flatten an element into path-value pairs (fallback for complex structures).
    fn flatten_element(&mut self, path: &str, element: &xbbg_core::Element) {
        let datatype = element.datatype();

        // For complex types, recurse into children
        if datatype.is_complex() {
            // If it's an array/sequence with values
            if element.is_array() {
                let n = element.len();
                for i in 0..n {
                    if let Some(child) = element.get_element(i) {
                        let child_path = if path.is_empty() {
                            format!("[{i}]")
                        } else {
                            format!("{path}[{i}]")
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            } else {
                // Iterate named children
                let n = element.num_children();
                for i in 0..n {
                    if let Some(child) = element.get_at(i) {
                        let name = child.name();
                        let child_path = if path.is_empty() {
                            name.as_str().to_string()
                        } else {
                            format!("{}.{}", path, name.as_str())
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            }
        } else {
            // Leaf value - add to columns
            if let Some(value) = element.get_value(0) {
                self.columns.append_str("path", path);

                // Convert value to string for generic representation
                let value_str = match &value {
                    xbbg_core::Value::String(s) | xbbg_core::Value::Enum(s) => s.to_string(),
                    xbbg_core::Value::Float64(f) => f.to_string(),
                    xbbg_core::Value::Int64(i) => i.to_string(),
                    xbbg_core::Value::Int32(i) => i.to_string(),
                    xbbg_core::Value::Bool(b) => b.to_string(),
                    xbbg_core::Value::Null => String::new(),
                    _ => format!("{:?}", value),
                };
                self.columns.append_str("value", &value_str);
                self.columns.end_row();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, StringArray};
    use serde_json::json;

    fn parse_batch(json: &str) -> RecordBatch {
        let (tx, _rx) = oneshot::channel();
        let state = BqlState::new(tx);
        state.parse_bql_json(json).expect("valid BQL JSON")
    }

    #[test]
    fn parses_bql_json_into_wide_batch() {
        let batch = parse_batch(
            &json!({
                "results": {
                    "px_last": {
                        "idColumn": { "values": ["AAPL US Equity", "MSFT US Equity"] },
                        "valuesColumn": { "values": [189.34, null] }
                    },
                    "name": {
                        "valuesColumn": { "values": ["Apple Inc.", "Microsoft"] }
                    }
                }
            })
            .to_string(),
        );

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let tickers = batch
            .column(batch.schema().index_of("ticker").expect("ticker column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker column");
        assert_eq!(tickers.value(0), "AAPL US Equity");
        assert_eq!(tickers.value(1), "MSFT US Equity");

        let px_last = batch
            .column(batch.schema().index_of("px_last").expect("px_last column"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("px_last column");
        assert_eq!(px_last.value(0), 189.34);
        assert!(px_last.is_null(1));

        let names = batch
            .column(batch.schema().index_of("name").expect("name column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        assert_eq!(names.value(0), "Apple Inc.");
        assert_eq!(names.value(1), "Microsoft");
    }

    #[test]
    fn parses_empty_bql_json_results_as_empty_batch() {
        let batch = parse_batch(r#"{"results": {}}"#);

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);

        let tickers = batch
            .column(batch.schema().index_of("ticker").expect("ticker column"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker column");
        assert_eq!(tickers.len(), 0);
    }
}
