use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use super::state::SubscriptionMetrics;
use super::SlabKey;

/// Overflow policy for slow consumers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop the newest data when buffer is full (default, non-blocking)
    #[default]
    DropNewest,
    /// Drop the oldest data when buffer is full (requires bounded ring buffer)
    DropOldest,
    /// Block the producer until space is available (use with caution)
    Block,
}

/// Why Bloomberg stopped a single subscribed topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionFailureKind {
    Failure,
    Terminated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicLifecycleState {
    Pending,
    Started,
    Streaming,
    Unsubscribing,
    Unsubscribed,
    Failed,
    Terminated,
}

impl TopicLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Started => "started",
            Self::Streaming => "streaming",
            Self::Unsubscribing => "unsubscribing",
            Self::Unsubscribed => "unsubscribed",
            Self::Failed => "failed",
            Self::Terminated => "terminated",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionLifecycleState {
    Starting,
    Up,
    Down,
    Terminated,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriptionRecoveryPolicy {
    #[default]
    None,
    Resubscribe,
}

impl SubscriptionRecoveryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Resubscribe => "resubscribe",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerHealth {
    #[default]
    Healthy,
    Degraded,
    Dead,
}

impl WorkerHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Dead => "dead",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub backoff_factor: f64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            initial_delay_ms: 1000,
            backoff_factor: 2.0,
            max_delay_ms: 30_000,
        }
    }
}

impl SessionLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Up => "up",
            Self::Down => "down",
            Self::Terminated => "terminated",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionEventCategory {
    Session,
    Service,
    Admin,
    Subscription,
    Lifecycle,
}

impl SubscriptionEventCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Service => "service",
            Self::Admin => "admin",
            Self::Subscription => "subscription",
            Self::Lifecycle => "lifecycle",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionEventLevel {
    Info,
    Warning,
    Error,
}

impl SubscriptionEventLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicStatusInfo {
    pub topic: String,
    pub state: TopicLifecycleState,
    pub last_change_us: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceStatusInfo {
    pub service: String,
    pub up: bool,
    pub last_change_us: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdminStatusInfo {
    pub slow_consumer_warning_active: bool,
    pub slow_consumer_warning_count: u64,
    pub slow_consumer_cleared_count: u64,
    pub data_loss_count: u64,
    pub last_warning_us: Option<i64>,
    pub last_cleared_us: Option<i64>,
    pub last_data_loss_us: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatusInfo {
    pub state: SessionLifecycleState,
    pub last_change_us: i64,
    pub disconnect_count: u64,
    pub reconnect_count: u64,
    pub recovery_policy: SubscriptionRecoveryPolicy,
    pub recovery_attempt_count: u64,
    pub recovery_success_count: u64,
    pub last_recovery_attempt_us: Option<i64>,
    pub last_recovery_success_us: Option<i64>,
    pub last_recovery_error: Option<String>,
}

impl Default for SessionStatusInfo {
    fn default() -> Self {
        Self {
            state: SessionLifecycleState::Starting,
            last_change_us: timestamp_now_us(),
            disconnect_count: 0,
            reconnect_count: 0,
            recovery_policy: SubscriptionRecoveryPolicy::None,
            recovery_attempt_count: 0,
            recovery_success_count: 0,
            last_recovery_attempt_us: None,
            last_recovery_success_us: None,
            last_recovery_error: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionEventInfo {
    pub at_us: i64,
    pub category: SubscriptionEventCategory,
    pub level: SubscriptionEventLevel,
    pub message_type: String,
    pub topic: Option<String>,
    pub detail: Option<String>,
}

const SUBSCRIPTION_EVENT_HISTORY_LIMIT: usize = 128;

fn timestamp_now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}

impl SubscriptionFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Terminated => "terminated",
        }
    }
}

/// Recorded non-fatal failure for a single topic in a multi-topic subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionFailureInfo {
    pub topic: String,
    pub reason: String,
    pub kind: SubscriptionFailureKind,
    pub at_us: i64,
}

/// Shared subscription status visible to worker and consumer-facing handles.
#[derive(Default)]
pub struct SubscriptionStatusState {
    keys: Vec<SlabKey>,
    topics: Vec<String>,
    topic_to_key: HashMap<String, SlabKey>,
    key_to_topic: HashMap<SlabKey, String>,
    metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>>,
    failures: Vec<SubscriptionFailureInfo>,
    topic_states: HashMap<String, TopicStatusInfo>,
    events: VecDeque<SubscriptionEventInfo>,
    session: SessionStatusInfo,
    services: HashMap<String, ServiceStatusInfo>,
    admin: AdminStatusInfo,
}

pub type SharedSubscriptionStatus = Arc<Mutex<SubscriptionStatusState>>;

impl SubscriptionStatusState {
    pub fn from_active(
        topics: Vec<String>,
        keys: Vec<SlabKey>,
        metrics: HashMap<SlabKey, Arc<SubscriptionMetrics>>,
        recovery_policy: SubscriptionRecoveryPolicy,
    ) -> Self {
        let mut status = Self {
            keys,
            topics,
            topic_to_key: HashMap::new(),
            key_to_topic: HashMap::new(),
            metrics,
            failures: Vec::new(),
            topic_states: HashMap::new(),
            events: VecDeque::with_capacity(SUBSCRIPTION_EVENT_HISTORY_LIMIT),
            session: SessionStatusInfo {
                state: SessionLifecycleState::Up,
                recovery_policy,
                ..SessionStatusInfo::default()
            },
            services: HashMap::new(),
            admin: AdminStatusInfo::default(),
        };
        let now = timestamp_now_us();
        let topics = status.topics.clone();
        let keys = status.keys.clone();
        for (topic, key) in topics.into_iter().zip(keys.into_iter()) {
            status.topic_to_key.insert(topic.clone(), key);
            status.key_to_topic.insert(key, topic.clone());
            status.topic_states.insert(
                topic.clone(),
                TopicStatusInfo {
                    topic,
                    state: TopicLifecycleState::Pending,
                    last_change_us: now,
                },
            );
        }
        status
    }

    pub fn add_active(
        &mut self,
        topics: &[String],
        keys: &[SlabKey],
        metrics: Vec<Arc<SubscriptionMetrics>>,
    ) {
        let now = timestamp_now_us();
        for ((topic, key), metric) in topics.iter().zip(keys.iter()).zip(metrics.into_iter()) {
            self.topic_to_key.insert(topic.clone(), *key);
            self.key_to_topic.insert(*key, topic.clone());
            self.topics.push(topic.clone());
            self.keys.push(*key);
            self.metrics.insert(*key, metric);
            self.topic_states.insert(
                topic.clone(),
                TopicStatusInfo {
                    topic: topic.clone(),
                    state: TopicLifecycleState::Pending,
                    last_change_us: now,
                },
            );
        }
    }

    pub fn remove_topic(&mut self, topic: &str) -> Option<SlabKey> {
        let key = self.topic_to_key.remove(topic)?;
        self.topics.retain(|existing| existing != topic);
        self.keys.retain(|existing| *existing != key);
        self.metrics.remove(&key);
        Some(key)
    }

    pub fn topic_for_key(&self, key: SlabKey) -> Option<&str> {
        self.key_to_topic.get(&key).map(String::as_str)
    }

    pub fn topic_statuses(&self) -> &HashMap<String, TopicStatusInfo> {
        &self.topic_states
    }

    pub fn session(&self) -> &SessionStatusInfo {
        &self.session
    }

    pub fn set_recovery_policy(&mut self, recovery_policy: SubscriptionRecoveryPolicy) {
        self.session.recovery_policy = recovery_policy;
    }

    pub fn services(&self) -> &HashMap<String, ServiceStatusInfo> {
        &self.services
    }

    pub fn admin(&self) -> &AdminStatusInfo {
        &self.admin
    }

    pub fn events(&self) -> &VecDeque<SubscriptionEventInfo> {
        &self.events
    }

    fn finalize_key(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.key_to_topic.remove(&key)?;
        self.topic_to_key.remove(&topic);
        self.topics.retain(|existing| existing != &topic);
        self.keys.retain(|existing| *existing != key);
        self.metrics.remove(&key);
        Some(topic)
    }

    pub fn push_event(
        &mut self,
        category: SubscriptionEventCategory,
        level: SubscriptionEventLevel,
        message_type: impl Into<String>,
        topic: Option<String>,
        detail: Option<String>,
    ) {
        if self.events.len() >= SUBSCRIPTION_EVENT_HISTORY_LIMIT {
            self.events.pop_front();
        }
        self.events.push_back(SubscriptionEventInfo {
            at_us: timestamp_now_us(),
            category,
            level,
            message_type: message_type.into(),
            topic,
            detail,
        });
    }

    fn update_topic_state(&mut self, topic: &str, state: TopicLifecycleState) {
        let now = timestamp_now_us();
        self.topic_states
            .entry(topic.to_string())
            .and_modify(|status| {
                status.state = state;
                status.last_change_us = now;
            })
            .or_insert_with(|| TopicStatusInfo {
                topic: topic.to_string(),
                state,
                last_change_us: now,
            });
    }

    pub fn mark_topic_started(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.topic_for_key(key)?.to_string();
        self.update_topic_state(&topic, TopicLifecycleState::Started);
        Some(topic)
    }

    pub fn mark_topic_streaming(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.topic_for_key(key)?.to_string();
        self.update_topic_state(&topic, TopicLifecycleState::Streaming);
        Some(topic)
    }

    pub fn mark_topic_unsubscribing(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.topic_for_key(key)?.to_string();
        let _ = self.remove_topic(&topic);
        self.update_topic_state(&topic, TopicLifecycleState::Unsubscribing);
        Some(topic)
    }

    pub fn mark_topic_unsubscribed(&mut self, key: SlabKey) -> Option<String> {
        let topic = self.finalize_key(key)?;
        self.update_topic_state(&topic, TopicLifecycleState::Unsubscribed);
        Some(topic)
    }

    pub fn record_failure(
        &mut self,
        key: SlabKey,
        reason: String,
        kind: SubscriptionFailureKind,
    ) -> Option<String> {
        let topic = self.finalize_key(key)?;
        let state = match kind {
            SubscriptionFailureKind::Failure => TopicLifecycleState::Failed,
            SubscriptionFailureKind::Terminated => TopicLifecycleState::Terminated,
        };
        self.update_topic_state(&topic, state);
        self.failures.push(SubscriptionFailureInfo {
            topic: topic.clone(),
            reason,
            kind,
            at_us: timestamp_now_us(),
        });
        Some(topic)
    }

    pub fn clear_active(&mut self) {
        self.keys.clear();
        self.topics.clear();
        self.topic_to_key.clear();
        self.key_to_topic.clear();
        self.metrics.clear();
    }

    pub fn keys(&self) -> &[SlabKey] {
        &self.keys
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub fn fields_metrics(&self) -> &HashMap<SlabKey, Arc<SubscriptionMetrics>> {
        &self.metrics
    }

    pub fn topic_to_key(&self) -> &HashMap<String, SlabKey> {
        &self.topic_to_key
    }

    pub fn failures(&self) -> &[SubscriptionFailureInfo] {
        &self.failures
    }

    pub fn has_active_topics(&self) -> bool {
        !self.keys.is_empty()
    }

    pub fn record_subscription_event(
        &mut self,
        message_type: &str,
        topic: Option<String>,
        detail: Option<String>,
        level: SubscriptionEventLevel,
    ) {
        self.push_event(
            SubscriptionEventCategory::Subscription,
            level,
            message_type,
            topic,
            detail,
        );
    }

    pub fn record_session_state(
        &mut self,
        state: SessionLifecycleState,
        message_type: &str,
        detail: Option<String>,
    ) {
        let now = timestamp_now_us();
        if self.session.state == SessionLifecycleState::Down && state == SessionLifecycleState::Up {
            self.session.reconnect_count += 1;
        }
        if state == SessionLifecycleState::Down {
            self.session.disconnect_count += 1;
        }
        self.session.state = state;
        self.session.last_change_us = now;
        let level = match state {
            SessionLifecycleState::Down | SessionLifecycleState::Terminated => {
                SubscriptionEventLevel::Error
            }
            _ => SubscriptionEventLevel::Info,
        };
        self.push_event(
            SubscriptionEventCategory::Session,
            level,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_service_state(
        &mut self,
        service: String,
        up: bool,
        message_type: &str,
        detail: Option<String>,
    ) {
        let now = timestamp_now_us();
        self.services.insert(
            service.clone(),
            ServiceStatusInfo {
                service: service.clone(),
                up,
                last_change_us: now,
            },
        );
        self.push_event(
            SubscriptionEventCategory::Service,
            if up {
                SubscriptionEventLevel::Info
            } else {
                SubscriptionEventLevel::Warning
            },
            message_type,
            Some(service),
            detail,
        );
    }

    pub fn record_admin_warning(&mut self, message_type: &str, detail: Option<String>) {
        self.admin.slow_consumer_warning_active = true;
        self.admin.slow_consumer_warning_count += 1;
        self.admin.last_warning_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Warning,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_admin_warning_cleared(&mut self, message_type: &str, detail: Option<String>) {
        self.admin.slow_consumer_warning_active = false;
        self.admin.slow_consumer_cleared_count += 1;
        self.admin.last_cleared_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Info,
            message_type,
            None,
            detail,
        );
    }

    pub fn record_admin_data_loss(&mut self, topic: Option<String>, detail: Option<String>) {
        self.admin.data_loss_count += 1;
        self.admin.last_data_loss_us = Some(timestamp_now_us());
        self.push_event(
            SubscriptionEventCategory::Admin,
            SubscriptionEventLevel::Warning,
            "DataLoss",
            topic,
            detail,
        );
    }

    pub fn record_recovery_attempt(&mut self, detail: Option<String>) {
        self.session.recovery_attempt_count += 1;
        self.session.last_recovery_attempt_us = Some(timestamp_now_us());
        self.session.last_recovery_error = None;
        self.push_event(
            SubscriptionEventCategory::Session,
            SubscriptionEventLevel::Info,
            "RecoveryAttempt",
            None,
            detail,
        );
    }

    pub fn record_recovery_success(&mut self, detail: Option<String>) {
        self.session.recovery_success_count += 1;
        self.session.last_recovery_success_us = Some(timestamp_now_us());
        self.session.last_recovery_error = None;
        self.push_event(
            SubscriptionEventCategory::Session,
            SubscriptionEventLevel::Info,
            "RecoverySucceeded",
            None,
            detail,
        );
    }

    pub fn record_recovery_error(&mut self, detail: String) {
        self.session.last_recovery_error = Some(detail.clone());
        self.push_event(
            SubscriptionEventCategory::Session,
            SubscriptionEventLevel::Warning,
            "RecoveryFailed",
            None,
            Some(detail),
        );
    }
}

impl std::str::FromStr for OverflowPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop_newest" | "dropnewest" => Ok(Self::DropNewest),
            "drop_oldest" | "dropoldest" => Ok(Self::DropOldest),
            "block" => Ok(Self::Block),
            _ => Err(format!(
                "unknown overflow policy '{}': expected drop_newest, drop_oldest, or block",
                s
            )),
        }
    }
}

impl std::fmt::Display for OverflowPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DropNewest => write!(f, "drop_newest"),
            Self::DropOldest => write!(f, "drop_oldest"),
            Self::Block => write!(f, "block"),
        }
    }
}
