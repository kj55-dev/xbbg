use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::*;
use tokio::sync::{mpsc, watch, Mutex};
use xbbg_async::engine::state::SubscriptionMetrics;
use xbbg_async::engine::{
    AdminStatusInfo, ServiceStatusInfo, SessionClaim, SessionStatusInfo, SharedSubscriptionStatus,
    SubscriptionCommandHandle, SubscriptionEventInfo, SubscriptionFailureInfo, SubscriptionStream,
    TopicStatusInfo,
};
use xbbg_async::OverflowPolicy;
use xbbg_core::BlpError;

use crate::{blp_async_error_to_pyerr, blp_error_to_pyerr, record_batch_to_pyarrow};

pub(crate) type StreamBatchResult = Result<arrow::record_batch::RecordBatch, BlpError>;
pub(crate) type StreamSender = mpsc::Sender<StreamBatchResult>;
pub(crate) type StreamReceiver = mpsc::Receiver<StreamBatchResult>;
pub(crate) type SharedStreamReceiver = Arc<Mutex<Option<StreamReceiver>>>;
pub(crate) type SubscriptionMetricsMap = HashMap<usize, Arc<SubscriptionMetrics>>;
pub(crate) type SubscriptionEventTuple =
    (i64, String, String, String, Option<String>, Option<String>);

macro_rules! py_dict {
    ($py:expr, $( $key:expr => $value:expr ),+ $(,)?) => {{
        let dict = PyDict::new($py);
        $(dict.set_item($key, $value)?;)+
        Ok(dict.into_any().unbind())
    }};
}

#[gen_stub_pyclass]
#[pyclass]
pub struct PySubscription {
    pub(crate) rx: SharedStreamReceiver,
    pub(crate) stream: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    pub(crate) ops: Arc<Mutex<()>>,
    pub(crate) close_signal: watch::Sender<bool>,
}

pub(crate) async fn wait_for_subscription_close(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }

    while close_rx.changed().await.is_ok() {
        if *close_rx.borrow() {
            return;
        }
    }
}

pub(crate) fn subscription_metrics_totals(
    metrics: &SubscriptionMetricsMap,
) -> (u64, u64, u64, bool, u64, u64, u64) {
    let messages_received = metrics
        .values()
        .map(|m| {
            m.messages_received
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .sum();
    let dropped_batches = metrics
        .values()
        .map(|m| m.dropped_batches.load(std::sync::atomic::Ordering::Relaxed))
        .sum();
    let batches_sent = metrics
        .values()
        .map(|m| m.batches_sent.load(std::sync::atomic::Ordering::Relaxed))
        .sum();
    let slow_consumer = metrics
        .values()
        .any(|m| m.slow_consumer.load(std::sync::atomic::Ordering::Relaxed));
    let data_loss_events = metrics
        .values()
        .map(|m| {
            m.data_loss_events
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .sum();
    let last_message_us = metrics
        .values()
        .map(|m| m.last_message_us.load(std::sync::atomic::Ordering::Relaxed))
        .max()
        .unwrap_or(0);
    let last_data_loss_us = metrics
        .values()
        .map(|m| {
            m.last_data_loss_us
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .max()
        .unwrap_or(0);

    (
        messages_received,
        dropped_batches,
        batches_sent,
        slow_consumer,
        data_loss_events,
        last_message_us,
        last_data_loss_us,
    )
}

/// Internal handle for subscription metadata and operations (without the receiver).
pub(crate) struct SubscriptionStreamHandle {
    pub(crate) tx: StreamSender,
    pub(crate) claim: Option<SessionClaim>,
    pub(crate) fields: Vec<String>,
    pub(crate) service: String,
    pub(crate) options: Vec<String>,
    pub(crate) flush_threshold: Option<usize>,
    pub(crate) overflow_policy: Option<OverflowPolicy>,
    pub(crate) _stream_capacity: Option<usize>,
    pub(crate) status: SharedSubscriptionStatus,
}

pub(crate) struct PendingAdd {
    pub(crate) command: SubscriptionCommandHandle,
    pub(crate) new_topics: Vec<String>,
    pub(crate) service: String,
    pub(crate) fields: Vec<String>,
    pub(crate) options: Vec<String>,
    pub(crate) flush_threshold: Option<usize>,
    pub(crate) overflow_policy: Option<OverflowPolicy>,
    pub(crate) tx: StreamSender,
    pub(crate) status: SharedSubscriptionStatus,
}

pub(crate) struct PendingRemove {
    pub(crate) command: SubscriptionCommandHandle,
    pub(crate) topics: Vec<String>,
    pub(crate) keys: Vec<usize>,
}

impl SubscriptionStreamHandle {
    pub(crate) fn prepare_add(&self, tickers: Vec<String>) -> PyResult<Option<PendingAdd>> {
        let claim = self
            .claim
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription already closed"))?;
        let command = claim.command_handle().map_err(blp_async_error_to_pyerr)?;

        let mut seen_topics = HashSet::new();
        let status = self.status.lock();
        let new_topics: Vec<String> = tickers
            .into_iter()
            .filter(|ticker| {
                !status.topic_to_key().contains_key(ticker) && seen_topics.insert(ticker.clone())
            })
            .collect();

        if new_topics.is_empty() {
            return Ok(None);
        }

        Ok(Some(PendingAdd {
            command,
            new_topics,
            service: self.service.clone(),
            fields: self.fields.clone(),
            options: self.options.clone(),
            flush_threshold: self.flush_threshold,
            overflow_policy: self.overflow_policy,
            tx: self.tx.clone(),
            status: self.status.clone(),
        }))
    }

    pub(crate) fn apply_add(
        &mut self,
        topics: &[String],
        new_keys: Vec<usize>,
        new_metrics: Vec<Arc<SubscriptionMetrics>>,
    ) {
        self.status
            .lock()
            .add_active(topics, &new_keys, new_metrics);
    }

    pub(crate) fn prepare_remove(&self, tickers: Vec<String>) -> PyResult<Option<PendingRemove>> {
        let claim = self
            .claim
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription already closed"))?;
        let command = claim.command_handle().map_err(blp_async_error_to_pyerr)?;

        let mut seen_keys = HashSet::new();
        let mut topics = Vec::new();
        let mut keys = Vec::new();
        let status = self.status.lock();

        for ticker in tickers {
            if let Some(&key) = status.topic_to_key().get(&ticker) {
                if seen_keys.insert(key) {
                    topics.push(ticker);
                    keys.push(key);
                }
            }
        }

        if keys.is_empty() {
            return Ok(None);
        }

        Ok(Some(PendingRemove {
            command,
            topics,
            keys,
        }))
    }

    pub(crate) fn apply_remove(&mut self, topics: &[String]) {
        let mut status = self.status.lock();
        for topic in topics {
            status.remove_topic(topic);
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct SubscriptionSnapshot {
    pub(crate) present: bool,
    pub(crate) topics: Vec<String>,
    pub(crate) fields: Vec<String>,
    pub(crate) is_active: bool,
    pub(crate) all_failed: bool,
    pub(crate) messages_received: u64,
    pub(crate) dropped_batches: u64,
    pub(crate) batches_sent: u64,
    pub(crate) slow_consumer: bool,
    pub(crate) data_loss_events: u64,
    pub(crate) last_message_us: u64,
    pub(crate) last_data_loss_us: u64,
    pub(crate) failures: Vec<SubscriptionFailureInfo>,
    pub(crate) topic_states: Vec<TopicStatusInfo>,
    pub(crate) session: SessionStatusInfo,
    pub(crate) services: Vec<ServiceStatusInfo>,
    pub(crate) admin: AdminStatusInfo,
    pub(crate) events: Vec<SubscriptionEventInfo>,
    pub(crate) effective_overflow_policy: String,
}

pub(crate) fn snapshot_from_stream(
    stream: &Arc<Mutex<Option<SubscriptionStreamHandle>>>,
) -> SubscriptionSnapshot {
    let guard = stream.blocking_lock();
    match guard.as_ref() {
        Some(handle) => {
            let status = handle.status.lock();
            let (
                messages_received,
                dropped_batches,
                batches_sent,
                slow_consumer,
                data_loss_events,
                last_message_us,
                last_data_loss_us,
            ) = subscription_metrics_totals(status.fields_metrics());
            let mut topic_states: Vec<TopicStatusInfo> =
                status.topic_statuses().values().cloned().collect();
            topic_states.sort_by(|left, right| left.topic.cmp(&right.topic));

            let mut services: Vec<ServiceStatusInfo> =
                status.services().values().cloned().collect();
            services.sort_by(|left, right| left.service.cmp(&right.service));

            SubscriptionSnapshot {
                present: true,
                topics: status.topics().to_vec(),
                fields: handle.fields.clone(),
                is_active: status.has_active_topics() && handle.claim.is_some(),
                all_failed: !status.has_active_topics() && !status.failures().is_empty(),
                messages_received,
                dropped_batches,
                batches_sent,
                slow_consumer,
                data_loss_events,
                last_message_us,
                last_data_loss_us,
                failures: status.failures().to_vec(),
                topic_states,
                session: status.session().clone(),
                services,
                admin: status.admin().clone(),
                events: status.events().iter().cloned().collect(),
                effective_overflow_policy: match handle
                    .overflow_policy
                    .unwrap_or(OverflowPolicy::DropNewest)
                {
                    OverflowPolicy::DropNewest => "drop_newest".to_string(),
                    OverflowPolicy::DropOldest => "drop_newest".to_string(),
                    OverflowPolicy::Block => "block".to_string(),
                },
            }
        }
        None => SubscriptionSnapshot::default(),
    }
}

pub(crate) fn py_subscription_from_stream<'py>(
    py: Python<'py>,
    stream: SubscriptionStream,
    fields: Vec<String>,
    stream_capacity: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let (rx, tx, claim, status, flush_threshold, overflow_policy, service, options) =
        stream.into_parts().map_err(blp_error_to_pyerr)?;

    let (close_signal, _) = watch::channel(false);
    let handle = SubscriptionStreamHandle {
        tx,
        claim: Some(claim),
        fields,
        service,
        options,
        flush_threshold,
        overflow_policy,
        _stream_capacity: stream_capacity,
        status,
    };

    Ok(Py::new(
        py,
        PySubscription {
            rx: Arc::new(Mutex::new(Some(rx))),
            stream: Arc::new(Mutex::new(Some(handle))),
            ops: Arc::new(Mutex::new(())),
            close_signal,
        },
    )?
    .into_any())
}

pub(crate) async fn add_tickers(
    stream: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    ops: Arc<Mutex<()>>,
    tickers: Vec<String>,
) -> PyResult<()> {
    let _op_guard = ops.lock().await;

    let pending = {
        let guard = stream.lock().await;
        let handle = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
        handle.prepare_add(tickers)?
    };

    let Some(pending) = pending else {
        return Ok(());
    };

    let (new_keys, new_metrics) = pending
        .command
        .add_topics(
            pending.service.clone(),
            pending.new_topics.clone(),
            pending.fields.clone(),
            pending.options.clone(),
            pending.flush_threshold,
            pending.overflow_policy,
            pending.tx.clone(),
            pending.status.clone(),
        )
        .await
        .map_err(blp_async_error_to_pyerr)?;

    let mut guard = stream.lock().await;
    let handle = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
    handle.apply_add(&pending.new_topics, new_keys, new_metrics);

    Ok(())
}

pub(crate) async fn remove_tickers(
    stream: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    ops: Arc<Mutex<()>>,
    tickers: Vec<String>,
) -> PyResult<()> {
    let _op_guard = ops.lock().await;

    let pending = {
        let guard = stream.lock().await;
        let handle = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
        handle.prepare_remove(tickers)?
    };

    let Some(pending) = pending else {
        return Ok(());
    };

    pending
        .command
        .unsubscribe(pending.keys.clone())
        .await
        .map_err(blp_async_error_to_pyerr)?;

    let mut guard = stream.lock().await;
    let handle = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
    handle.apply_remove(&pending.topics);
    Ok(())
}

pub(crate) async fn unsubscribe_subscription(
    stream_arc: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    rx_arc: SharedStreamReceiver,
    ops: Arc<Mutex<()>>,
    close_signal: watch::Sender<bool>,
    drain: bool,
) -> PyResult<Py<PyAny>> {
    let _op_guard = ops.lock().await;
    let _ = close_signal.send(true);

    let handle = {
        let mut guard = stream_arc.lock().await;
        guard.take()
    };

    let mut remaining = Vec::new();
    if drain {
        let rx = {
            let mut guard = rx_arc.lock().await;
            guard.take()
        };
        if let Some(mut rx) = rx {
            while let Ok(item) = rx.try_recv() {
                if let Ok(batch) = item {
                    remaining.push(batch);
                }
            }
        }
    }

    if let Some(mut handle) = handle {
        if let Some(claim) = handle.claim.take() {
            let keys = handle.status.lock().keys().to_vec();
            if !keys.is_empty() {
                let _ = claim.unsubscribe(keys).await;
            }
        }
    }

    if !remaining.is_empty() {
        Python::attach(|py| {
            let list = PyList::empty(py);
            for batch in remaining {
                let py_batch = record_batch_to_pyarrow(py, batch)?;
                list.append(py_batch)?;
            }
            Ok(list.into_any().unbind())
        })
    } else {
        Python::attach(|py| Ok(py.None()))
    }
}

pub(crate) fn stats_to_py(py: Python<'_>, snapshot: SubscriptionSnapshot) -> PyResult<Py<PyAny>> {
    py_dict!(
        py,
        "messages_received" => snapshot.messages_received,
        "dropped_batches" => snapshot.dropped_batches,
        "batches_sent" => snapshot.batches_sent,
        "slow_consumer" => snapshot.slow_consumer,
        "data_loss_events" => snapshot.data_loss_events,
        "last_message_us" => snapshot.last_message_us,
        "last_data_loss_us" => snapshot.last_data_loss_us,
        "effective_overflow_policy" => snapshot.effective_overflow_policy,
    )
}

pub(crate) fn session_status_to_py(
    py: Python<'_>,
    snapshot: SubscriptionSnapshot,
) -> PyResult<Py<PyAny>> {
    py_dict!(
        py,
        "state" => snapshot.session.state.as_str(),
        "last_change_us" => snapshot.session.last_change_us,
        "disconnect_count" => snapshot.session.disconnect_count,
        "reconnect_count" => snapshot.session.reconnect_count,
        "recovery_policy" => snapshot.session.recovery_policy.as_str(),
        "recovery_attempt_count" => snapshot.session.recovery_attempt_count,
        "recovery_success_count" => snapshot.session.recovery_success_count,
        "last_recovery_attempt_us" => snapshot.session.last_recovery_attempt_us,
        "last_recovery_success_us" => snapshot.session.last_recovery_success_us,
        "last_recovery_error" => snapshot.session.last_recovery_error,
    )
}

pub(crate) fn admin_status_to_py(
    py: Python<'_>,
    snapshot: SubscriptionSnapshot,
) -> PyResult<Py<PyAny>> {
    py_dict!(
        py,
        "slow_consumer_warning_active" => snapshot.admin.slow_consumer_warning_active,
        "slow_consumer_warning_count" => snapshot.admin.slow_consumer_warning_count,
        "slow_consumer_cleared_count" => snapshot.admin.slow_consumer_cleared_count,
        "data_loss_count" => snapshot.admin.data_loss_count,
        "last_warning_us" => snapshot.admin.last_warning_us,
        "last_cleared_us" => snapshot.admin.last_cleared_us,
        "last_data_loss_us" => snapshot.admin.last_data_loss_us,
    )
}

pub(crate) fn service_status_tuples(snapshot: SubscriptionSnapshot) -> Vec<(String, bool, i64)> {
    snapshot
        .services
        .into_iter()
        .map(|service| (service.service, service.up, service.last_change_us))
        .collect()
}

pub(crate) fn topic_state_tuples(snapshot: SubscriptionSnapshot) -> Vec<(String, String, i64)> {
    snapshot
        .topic_states
        .into_iter()
        .map(|topic| {
            (
                topic.topic,
                topic.state.as_str().to_string(),
                topic.last_change_us,
            )
        })
        .collect()
}

pub(crate) fn event_tuples(snapshot: SubscriptionSnapshot) -> Vec<SubscriptionEventTuple> {
    snapshot
        .events
        .into_iter()
        .map(|event| {
            (
                event.at_us,
                event.category.as_str().to_string(),
                event.level.as_str().to_string(),
                event.message_type,
                event.topic,
                event.detail,
            )
        })
        .collect()
}

pub(crate) fn failed_ticker_list(snapshot: SubscriptionSnapshot) -> Vec<String> {
    snapshot
        .failures
        .into_iter()
        .map(|failure| failure.topic)
        .collect()
}

pub(crate) fn failure_tuples(snapshot: SubscriptionSnapshot) -> Vec<(String, String, String)> {
    snapshot
        .failures
        .into_iter()
        .map(|failure| {
            (
                failure.topic,
                failure.reason,
                failure.kind.as_str().to_string(),
            )
        })
        .collect()
}

pub(crate) fn subscription_repr(snapshot: &SubscriptionSnapshot) -> String {
    if snapshot.present {
        format!(
            "Subscription(tickers={:?}, fields={:?}, active={})",
            snapshot.topics, snapshot.fields, snapshot.is_active
        )
    } else {
        "Subscription(closed)".to_string()
    }
}

impl PySubscription {
    fn snapshot(&self, py: Python<'_>) -> SubscriptionSnapshot {
        let stream = self.stream.clone();
        py.detach(move || snapshot_from_stream(&stream))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySubscription {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let close_signal = self.close_signal.clone();

        future_into_py(py, async move {
            let mut close_rx = close_signal.subscribe();
            let item = {
                let mut guard = rx.lock().await;
                let rx_ref = guard
                    .as_mut()
                    .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
                tokio::select! {
                    item = rx_ref.recv() => Ok(item),
                    _ = wait_for_subscription_close(&mut close_rx) => Err(()),
                }
            };

            match item {
                Ok(Some(Ok(batch))) => Python::attach(|py| record_batch_to_pyarrow(py, batch)),
                Ok(Some(Err(blp_err))) => Err(blp_error_to_pyerr(blp_err)),
                Ok(None) => Err(PyStopAsyncIteration::new_err("subscription ended")),
                Err(()) => Err(PyStopAsyncIteration::new_err("subscription closed")),
            }
        })
    }

    #[pyo3(signature = (tickers))]
    fn add<'py>(&self, py: Python<'py>, tickers: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        let ops = self.ops.clone();

        xbbg_log::debug!(tickers = ?tickers, "PySubscription: adding tickers");

        future_into_py(py, async move { add_tickers(stream, ops, tickers).await })
    }

    #[pyo3(signature = (tickers))]
    fn remove<'py>(&self, py: Python<'py>, tickers: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        let ops = self.ops.clone();

        xbbg_log::debug!(tickers = ?tickers, "PySubscription: removing tickers");

        future_into_py(
            py,
            async move { remove_tickers(stream, ops, tickers).await },
        )
    }

    #[getter]
    fn tickers(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).topics
    }

    #[getter]
    fn fields(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).fields
    }

    #[getter]
    fn is_active(&self, py: Python<'_>) -> bool {
        self.snapshot(py).is_active
    }

    #[getter]
    fn all_failed(&self, py: Python<'_>) -> bool {
        self.snapshot(py).all_failed
    }

    #[getter]
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        stats_to_py(py, self.snapshot(py))
    }

    #[getter]
    fn session_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        session_status_to_py(py, self.snapshot(py))
    }

    #[getter]
    fn admin_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        admin_status_to_py(py, self.snapshot(py))
    }

    #[getter]
    fn service_status(&self, py: Python<'_>) -> Vec<(String, bool, i64)> {
        service_status_tuples(self.snapshot(py))
    }

    #[getter]
    fn topic_states(&self, py: Python<'_>) -> Vec<(String, String, i64)> {
        topic_state_tuples(self.snapshot(py))
    }

    #[getter]
    fn events(&self, py: Python<'_>) -> Vec<SubscriptionEventTuple> {
        event_tuples(self.snapshot(py))
    }

    #[getter]
    fn failed_tickers(&self, py: Python<'_>) -> Vec<String> {
        failed_ticker_list(self.snapshot(py))
    }

    #[getter]
    fn failures(&self, py: Python<'_>) -> Vec<(String, String, String)> {
        failure_tuples(self.snapshot(py))
    }

    #[pyo3(signature = (drain = false))]
    fn unsubscribe<'py>(&self, py: Python<'py>, drain: bool) -> PyResult<Bound<'py, PyAny>> {
        let stream_arc = self.stream.clone();
        let rx_arc = self.rx.clone();
        let ops = self.ops.clone();
        let close_signal = self.close_signal.clone();

        xbbg_log::debug!(drain = drain, "PySubscription: unsubscribing");

        future_into_py(py, async move {
            unsubscribe_subscription(stream_arc, rx_arc, ops, close_signal, drain).await
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_val: Option<Bound<'py, PyAny>>,
        _exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.unsubscribe(py, false)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        subscription_repr(&self.snapshot(py))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyDict;
    use xbbg_async::engine::{SessionLifecycleState, SubscriptionRecoveryPolicy};

    #[test]
    fn subscription_status_dicts_preserve_expected_fields() {
        Python::initialize();
        Python::attach(|py| {
            let mut snapshot = SubscriptionSnapshot::default();
            snapshot.messages_received = 12;
            snapshot.dropped_batches = 3;
            snapshot.batches_sent = 7;
            snapshot.slow_consumer = true;
            snapshot.data_loss_events = 2;
            snapshot.last_message_us = 123;
            snapshot.last_data_loss_us = 456;
            snapshot.effective_overflow_policy = "block".to_string();

            snapshot.session = SessionStatusInfo {
                state: SessionLifecycleState::Up,
                last_change_us: 42,
                disconnect_count: 1,
                reconnect_count: 2,
                recovery_policy: SubscriptionRecoveryPolicy::Resubscribe,
                recovery_attempt_count: 3,
                recovery_success_count: 4,
                last_recovery_attempt_us: Some(5),
                last_recovery_success_us: Some(6),
                last_recovery_error: Some("boom".to_string()),
            };

            snapshot.admin = AdminStatusInfo {
                slow_consumer_warning_active: true,
                slow_consumer_warning_count: 8,
                slow_consumer_cleared_count: 9,
                data_loss_count: 10,
                last_warning_us: Some(11),
                last_cleared_us: Some(12),
                last_data_loss_us: Some(13),
            };

            let stats = stats_to_py(py, snapshot.clone()).expect("stats dict");
            let stats = stats.bind(py).cast::<PyDict>().expect("stats dict cast");
            assert_eq!(
                stats
                    .get_item("messages_received")
                    .expect("messages_received")
                    .expect("messages_received value")
                    .extract::<u64>()
                    .expect("messages_received extract"),
                12
            );
            assert_eq!(
                stats
                    .get_item("effective_overflow_policy")
                    .expect("effective_overflow_policy")
                    .expect("effective_overflow_policy value")
                    .extract::<String>()
                    .expect("effective_overflow_policy extract"),
                "block"
            );

            let session = session_status_to_py(py, snapshot.clone()).expect("session dict");
            let session = session
                .bind(py)
                .cast::<PyDict>()
                .expect("session dict cast");
            assert_eq!(
                session
                    .get_item("state")
                    .expect("state")
                    .expect("state value")
                    .extract::<String>()
                    .expect("state extract"),
                "up"
            );
            assert_eq!(
                session
                    .get_item("last_recovery_error")
                    .expect("last_recovery_error")
                    .expect("last_recovery_error value")
                    .extract::<String>()
                    .expect("last_recovery_error extract"),
                "boom"
            );

            let admin = admin_status_to_py(py, snapshot).expect("admin dict");
            let admin = admin.bind(py).cast::<PyDict>().expect("admin dict cast");
            assert_eq!(
                admin
                    .get_item("slow_consumer_warning_count")
                    .expect("slow_consumer_warning_count")
                    .expect("slow_consumer_warning_count value")
                    .extract::<u64>()
                    .expect("slow_consumer_warning_count extract"),
                8
            );
            assert_eq!(
                admin
                    .get_item("last_data_loss_us")
                    .expect("last_data_loss_us")
                    .expect("last_data_loss_us value")
                    .extract::<Option<i64>>()
                    .expect("last_data_loss_us extract"),
                Some(13)
            );
        });
    }
}
