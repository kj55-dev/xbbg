//! Subscription state with Arrow builders for real-time data.
//!
//! Extracts subscription messages directly from Bloomberg Elements
//! without JSON intermediate serialization. Uses dynamic type dispatch
//! to preserve all Bloomberg types (string, int, float, datetime, etc.).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use xbbg_core::{BlpError, Message, Value};

use super::super::OverflowPolicy;
use super::typed_builder::{ArrowType, TypedBuilder};

pub struct SubscriptionMetrics {
    pub messages_received: Arc<AtomicU64>,
    pub dropped_batches: Arc<AtomicU64>,
    pub batches_sent: Arc<AtomicU64>,
    pub slow_consumer: Arc<AtomicBool>,
    pub data_loss_events: Arc<AtomicU64>,
    pub last_message_us: Arc<AtomicU64>,
    pub last_data_loss_us: Arc<AtomicU64>,
}

/// State for a single subscription, owned by PumpA.
pub struct SubscriptionState {
    /// Topic string (e.g., "IBM US Equity")
    pub topic: Arc<str>,
    /// Field names as strings (for schema and lookup)
    pub field_strings: Vec<String>,
    /// Timestamp builder (event time)
    pub timestamp_builder: TimestampMicrosecondBuilder,
    /// Topic builder (repeated for each row)
    pub topic_builder: StringBuilder,
    /// Field value builders — None until type is inferred from first non-null value.
    /// This preserves Bloomberg's native types (Int32, Int64, Float64, String, Date, etc.)
    /// instead of forcing everything through Float64.
    pub field_builders: Vec<Option<TypedBuilder>>,
    /// Stream to send RecordBatches (or errors for subscription failures)
    pub stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    /// Number of pending rows before flush
    pub pending_count: usize,
    /// Flush threshold
    pub flush_threshold: usize,
    /// Slow consumer flag (DATALOSS received)
    pub slow_consumer: bool,
    /// Overflow policy for slow consumers
    pub overflow_policy: OverflowPolicy,
    /// Dropped batch count (for metrics)
    pub dropped_batches: u64,
    pub metrics: Arc<SubscriptionMetrics>,
    /// Cached schema — invalidated when a field type is first inferred.
    cached_schema: Option<Arc<Schema>>,
    /// Whether at least one data message has been observed.
    has_received_data: bool,
    /// Suppress stream-closed warnings during expected shutdown paths.
    suppress_closed_warning: bool,
}

impl SubscriptionState {
    /// Create a new subscription state with default overflow policy.
    pub fn new(
        topic: String,
        fields: Vec<String>,
        stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
        flush_threshold: usize,
    ) -> Self {
        Self::with_policy(
            topic,
            fields,
            stream,
            flush_threshold,
            OverflowPolicy::default(),
        )
    }

    /// Create a new subscription state with specified overflow policy.
    pub fn with_policy(
        topic: String,
        fields: Vec<String>,
        stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
        flush_threshold: usize,
        overflow_policy: OverflowPolicy,
    ) -> Self {
        let field_builders = fields.iter().map(|_| None).collect();
        let metrics = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });

        Self {
            topic: topic.into(),
            field_strings: fields,
            timestamp_builder: TimestampMicrosecondBuilder::new(),
            topic_builder: StringBuilder::new(),
            field_builders,
            stream,
            pending_count: 0,
            flush_threshold,
            slow_consumer: false,
            overflow_policy,
            dropped_batches: 0,
            metrics,
            cached_schema: None,
            has_received_data: false,
            suppress_closed_warning: false,
        }
    }

    /// Process a SUBSCRIPTION_DATA message using Element API.
    ///
    /// Uses dynamic type dispatch (`get_value`) to preserve Bloomberg's native types.
    /// Field types are inferred on first non-null value and locked in for the
    /// lifetime of the subscription. String, Date, Datetime, Bool, Int, Float
    /// are all preserved — no more Float64-only extraction.
    ///
    /// Timestamps use Bloomberg SDK receive time when available (requires
    /// `setRecordSubscriptionDataReceiveTimes(true)`), falling back to
    /// `SystemTime::now()` if not enabled.
    pub fn on_message(&mut self, msg: &Message) -> bool {
        // Use Bloomberg SDK receive time if available, fallback to system time
        let timestamp = msg
            .time_received_us()
            .unwrap_or_else(Self::current_timestamp_us);

        self.timestamp_builder.append_value(timestamp);
        self.topic_builder.append_value(self.topic.as_ref());

        // Extract each field value using dynamic type dispatch
        let elem = msg.elements();
        for i in 0..self.field_strings.len() {
            let (field_value, field_present) = {
                let field_name = &self.field_strings[i];
                match elem.get_by_str(field_name) {
                    Some(field_elem) => (field_elem.get_value(0), true),
                    None => (None, false),
                }
            };
            self.append_field_value(i, field_value, field_present);
        }

        self.pending_count += 1;
        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_message_us
            .store(timestamp as u64, Ordering::Relaxed);

        let first_message = !self.has_received_data;
        self.has_received_data = true;

        // Auto-flush if threshold reached
        if self.pending_count >= self.flush_threshold {
            self.flush();
        }

        first_message
    }

    /// Handle DATALOSS indicator.
    pub fn on_dataloss(&mut self, timestamp_us: Option<i64>) {
        self.set_slow_consumer(true);
        self.metrics
            .data_loss_events
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.last_data_loss_us.store(
            timestamp_us.unwrap_or_default().max(0) as u64,
            Ordering::Relaxed,
        );
        xbbg_log::warn!(topic = %self.topic, "DATALOSS detected - slow consumer");
    }

    pub fn clear_slow_consumer(&mut self) {
        self.set_slow_consumer(false);
    }

    pub fn mark_closing(&mut self) {
        self.suppress_closed_warning = true;
    }

    /// Flush pending rows as a RecordBatch.
    pub fn flush(&mut self) {
        if self.pending_count == 0 {
            return;
        }

        // Build fixed arrays
        let timestamp_array = self.timestamp_builder.finish();
        let topic_array = self.topic_builder.finish();

        // Build field arrays — use TypedBuilder where available, String nulls otherwise
        let field_arrays: Vec<ArrayRef> = self
            .field_builders
            .iter_mut()
            .map(|builder_opt| {
                if let Some(builder) = builder_opt {
                    builder.finish()
                } else {
                    // Field was never non-null in this batch — produce Utf8 column of all nulls
                    let mut sb = StringBuilder::new();
                    for _ in 0..self.pending_count {
                        sb.append_null();
                    }
                    Arc::new(sb.finish()) as ArrayRef
                }
            })
            .collect();

        // Get or build schema (cached after first build)
        let schema = self.get_or_build_schema();

        // Build columns
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            vec![Arc::new(timestamp_array), Arc::new(topic_array)];
        columns.extend(field_arrays);

        // Create RecordBatch
        match RecordBatch::try_new(schema, columns) {
            Ok(batch) => {
                self.send_batch(batch);
            }
            Err(e) => {
                xbbg_log::error!(topic = %self.topic, error = %e, "failed to create RecordBatch");
            }
        }

        self.pending_count = 0;
    }

    /// Get or build the Arrow schema, caching it for reuse.
    ///
    /// The schema is invalidated whenever a new field type is inferred
    /// (when a previously-null field gets its first non-null value).
    fn get_or_build_schema(&mut self) -> Arc<Schema> {
        if let Some(ref schema) = self.cached_schema {
            return schema.clone();
        }

        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("topic", DataType::Utf8, false),
        ];

        for (i, name) in self.field_strings.iter().enumerate() {
            let dt = self.field_builders[i]
                .as_ref()
                .map(|b| b.data_type())
                .unwrap_or(DataType::Utf8); // Unknown fields default to string
            fields.push(Field::new(name.as_str(), dt, true));
        }

        let schema = Arc::new(Schema::new(fields));
        self.cached_schema = Some(schema.clone());
        schema
    }

    /// Send an error to the consumer.
    ///
    /// Used for subscription failures, session termination, etc.
    /// Uses try_send to avoid blocking the worker thread.
    pub fn fail(&self, error: BlpError) {
        let _ = self.stream.try_send(Err(error));
    }

    /// Send a batch according to the configured overflow policy.
    ///
    /// NOTE: `DropOldest` is still degraded to `DropNewest` (needs ring buffer).
    /// `Block` now works properly using `blocking_send`.
    fn send_batch(&mut self, batch: RecordBatch) {
        match self.overflow_policy {
            OverflowPolicy::Block => {
                // blocking_send is designed for sync contexts (subscription worker thread).
                // Blocks until space is available or the receiver is dropped.
                if self.stream.blocking_send(Ok(batch)).is_err() {
                    self.warn_stream_closed();
                } else {
                    self.record_batch_sent();
                }
            }
            _ => {
                // DropNewest and DropOldest both use try_send.
                // DropOldest is degraded to DropNewest — proper ring buffer not yet implemented.
                match self.stream.try_send(Ok(batch)) {
                    Ok(()) => {
                        self.record_batch_sent();
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.dropped_batches += 1;
                        self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                        xbbg_log::warn!(
                            topic = %self.topic,
                            dropped = self.dropped_batches,
                            policy = self.overflow_policy_label(),
                            "stream full - dropping batch"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.warn_stream_closed();
                    }
                }
            }
        }
    }

    fn current_timestamp_us() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    fn set_slow_consumer(&mut self, active: bool) {
        self.slow_consumer = active;
        self.metrics.slow_consumer.store(active, Ordering::Relaxed);
    }

    fn record_batch_sent(&self) {
        self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn warn_stream_closed(&self) {
        if !self.suppress_closed_warning {
            xbbg_log::warn!(topic = %self.topic, "stream closed");
        }
    }

    fn overflow_policy_label(&self) -> &'static str {
        match self.overflow_policy {
            OverflowPolicy::DropNewest => "DropNewest",
            OverflowPolicy::DropOldest => "DropOldest (degraded to DropNewest)",
            OverflowPolicy::Block => "Block",
        }
    }

    fn append_field_value(&mut self, index: usize, value: Option<Value<'_>>, field_present: bool) {
        if let Some(builder) = &mut self.field_builders[index] {
            if field_present {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
            return;
        }

        if !field_present {
            return;
        }

        let Some(value_ref) = value.as_ref() else {
            return;
        };
        if value_ref.is_null() {
            return;
        }

        let mut builder = TypedBuilder::new(ArrowType::from_value(value_ref));
        for _ in 0..self.pending_count {
            builder.append_null();
        }
        builder.append_value(value);
        self.field_builders[index] = Some(builder);
        self.cached_schema = None; // Schema needs rebuild
    }
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        // Flush any remaining rows
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let mut timestamp_builder = TimestampMicrosecondBuilder::new();
        timestamp_builder.append_value(1);
        let mut topic_builder = StringBuilder::new();
        topic_builder.append_value("topic");

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("topic", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_builder.finish()) as ArrayRef,
                Arc::new(topic_builder.finish()) as ArrayRef,
            ],
        )
        .expect("valid test batch")
    }

    #[test]
    fn slow_consumer_helpers_update_state_and_metrics() {
        let (stream, _rx) = mpsc::channel(1);
        let mut state = SubscriptionState::with_policy(
            "IBM US Equity".to_string(),
            vec!["PX_LAST".to_string()],
            stream,
            1,
            OverflowPolicy::default(),
        );

        state.on_dataloss(Some(123));
        assert!(state.slow_consumer);
        assert!(state.metrics.slow_consumer.load(Ordering::Relaxed));
        assert_eq!(state.metrics.data_loss_events.load(Ordering::Relaxed), 1);
        assert_eq!(state.metrics.last_data_loss_us.load(Ordering::Relaxed), 123);

        state.clear_slow_consumer();
        assert!(!state.slow_consumer);
        assert!(!state.metrics.slow_consumer.load(Ordering::Relaxed));
    }

    #[test]
    fn append_field_value_infers_builder_and_backfills_nulls() {
        let (stream, _rx) = mpsc::channel(1);
        let mut state = SubscriptionState::with_policy(
            "IBM US Equity".to_string(),
            vec!["PX_LAST".to_string()],
            stream,
            1,
            OverflowPolicy::default(),
        );
        state.pending_count = 2;
        state.cached_schema = Some(Arc::new(Schema::empty()));

        state.append_field_value(0, Some(Value::Int64(42)), true);

        let builder = state.field_builders[0].as_ref().expect("builder created");
        assert_eq!(builder.len(), 3);
        assert!(state.cached_schema.is_none());
    }

    #[test]
    fn append_field_value_appends_nulls_for_missing_present_builder() {
        let (stream, _rx) = mpsc::channel(1);
        let mut state = SubscriptionState::with_policy(
            "IBM US Equity".to_string(),
            vec!["PX_LAST".to_string()],
            stream,
            1,
            OverflowPolicy::default(),
        );
        state.field_builders[0] = Some(TypedBuilder::new(ArrowType::Int64));

        state.append_field_value(0, None, false);

        let builder = state.field_builders[0].as_ref().expect("builder kept");
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn send_batch_updates_metrics_for_success_and_full_channel() {
        let (stream, _rx) = mpsc::channel(1);
        let mut state = SubscriptionState::with_policy(
            "IBM US Equity".to_string(),
            vec![],
            stream,
            1,
            OverflowPolicy::DropNewest,
        );
        let batch = test_batch();

        state.send_batch(batch.clone());
        assert_eq!(state.metrics.batches_sent.load(Ordering::Relaxed), 1);
        assert_eq!(state.dropped_batches, 0);

        state.send_batch(batch);
        assert_eq!(state.metrics.batches_sent.load(Ordering::Relaxed), 1);
        assert_eq!(state.dropped_batches, 1);
        assert_eq!(state.metrics.dropped_batches.load(Ordering::Relaxed), 1);
    }
}
