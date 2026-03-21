//! Bloomberg subscription management

use std::ffi::CString;

use crate::correlation::CorrelationId;
use crate::errors::{BlpError, Result};

/// Subscription list for market data subscriptions.
///
/// A SubscriptionList is used to subscribe to real-time market data updates.
/// Each subscription has a topic (e.g., "IBM US Equity"), fields (e.g., ["LAST_PRICE"]),
/// and a correlation ID for tracking updates.
///
/// # Examples
///
/// ```ignore
/// let mut subs = SubscriptionList::new();
///
/// // Add subscription
/// subs.add(
///     "IBM US Equity",
///     &["LAST_PRICE", "BID", "ASK"],
///     "",  // No options
///     &CorrelationId::new_int(1),
/// )?;
///
/// // Subscribe
/// session.subscribe(&subs)?;
///
/// // Process subscription data
/// loop {
///     if let Ok(ev) = session.next_event(None) {
///         if ev.event_type() == EventType::SubscriptionData {
///             for msg in ev.messages() {
///                 // Process real-time update...
///             }
///         }
///     }
/// }
/// ```
pub struct SubscriptionList {
    ptr: *mut crate::ffi::blpapi_SubscriptionList_t,
}

// SAFETY: SubscriptionList can be sent between threads
unsafe impl Send for SubscriptionList {}

// SAFETY: SubscriptionList can be shared between threads
unsafe impl Sync for SubscriptionList {}

impl SubscriptionList {
    /// Create a new empty subscription list.
    ///
    /// Add subscriptions with `add()` before passing to `Session::subscribe()`.
    pub fn new() -> Self {
        // SAFETY: We're calling the Bloomberg API to create a subscription list
        let ptr = unsafe { crate::ffi::blpapi_SubscriptionList_create() };

        // Note: Bloomberg API may return null on failure, but we'll handle that gracefully
        Self { ptr }
    }

    /// Add a subscription to the list.
    ///
    /// # Arguments
    /// * `topic` - The subscription topic (e.g., "IBM US Equity")
    /// * `fields` - The fields to subscribe to (e.g., ["LAST_PRICE", "BID", "ASK"])
    /// * `options` - Subscription options (e.g., "interval=5" for delayed updates, "" for none)
    /// * `cid` - Correlation ID for tracking this subscription
    ///
    /// # Returns
    /// Ok(()) on success, Err on failure
    pub fn add(
        &mut self,
        topic: &str,
        fields: &[&str],
        options: &str,
        cid: &CorrelationId,
    ) -> Result<()> {
        if self.ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "subscription list not initialized".into(),
            });
        }

        let c_topic = cstring("topic", topic)?;

        let c_fields = cstring_vec("field", fields)?;

        let field_ptrs = ptrs(&c_fields);

        let options_vec = split_options(options);

        let c_options = cstring_vec("option", &options_vec)?;

        let option_ptrs = ptrs(&c_options);

        // Convert correlation ID to FFI format
        let cid_ffi = cid.to_ffi();

        // SAFETY: We're calling the Bloomberg API with valid pointers
        // - self.ptr is guaranteed non-null (checked above)
        // - c_topic is a valid C string
        // - field_ptrs and option_ptrs are valid arrays
        // - cid_ffi is a valid correlation ID
        let rc = unsafe {
            crate::ffi::blpapi_SubscriptionList_add(
                self.ptr,
                c_topic.as_ptr(),
                &cid_ffi,
                field_ptrs.as_ptr() as *mut *const i8,
                option_ptrs.as_ptr() as *mut *const i8,
                field_ptrs.len(),
                option_ptrs.len(),
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_SubscriptionList_add failed with rc={}", rc),
            });
        }

        Ok(())
    }

    /// Get the raw pointer (internal use only)
    pub(crate) fn as_ptr(&self) -> *mut crate::ffi::blpapi_SubscriptionList_t {
        self.ptr
    }
}

fn cstring(label: &str, value: &str) -> Result<CString> {
    CString::new(value).map_err(|e| BlpError::InvalidArgument {
        detail: format!("invalid {label}: {e}"),
    })
}

fn cstring_vec(label: &str, values: &[&str]) -> Result<Vec<CString>> {
    values.iter().map(|value| cstring(label, value)).collect()
}

fn ptrs(values: &[CString]) -> Vec<*const i8> {
    values.iter().map(|value| value.as_ptr()).collect()
}

fn split_options(options: &str) -> Vec<&str> {
    if options.is_empty() {
        Vec::new()
    } else {
        options.split(',').map(|option| option.trim()).collect()
    }
}

impl Default for SubscriptionList {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SubscriptionList {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: We're calling the Bloomberg API to destroy the subscription list
            unsafe {
                crate::ffi::blpapi_SubscriptionList_destroy(self.ptr);
            }
            self.ptr = std::ptr::null_mut();
        }
    }
}
