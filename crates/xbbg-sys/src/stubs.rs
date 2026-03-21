//! Stub implementations for APIs missing from datamock.
//!
//! These stubs provide minimal implementations for APIs that datamock doesn't have
//! but xbbg-core might call. They enable testing without full API coverage.
//!
//! NOTE: Functions with signature mismatches (Element_getElement, Message_elements, etc.)
//! are now provided by shim.rs instead of here.

use std::ffi::{c_char, c_void};
use std::ptr;

macro_rules! ffi_stub {
    (
        $(#[$meta:meta])*
        fn $name:ident(
            $($arg:ident : $ty:ty),* $(,)?
        ) -> $ret:ty => $body:expr $(;)?) => {
        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name($($arg: $ty),*) -> $ret {
            $body
        }
    };
    (
        $(#[$meta:meta])*
        fn $name:ident(
            $($arg:ident : $ty:ty),* $(,)?
        ) => $body:expr $(;)?) => {
        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name($($arg: $ty),*) {
            $body
        }
    };
}

// ============================================================================
// Opaque type stubs
// ============================================================================

/// Identity type stub (datamock doesn't have Identity)
#[repr(C)]
pub struct blpapi_Identity_t {
    _private: [u8; 0],
}

// ============================================================================
// Name function stubs
// ============================================================================

ffi_stub!(
    /// Duplicate a Name (mock: just return same pointer, no actual duplication)
    fn blpapi_Name_duplicate(name: *const crate::blpapi_Name_t) -> *mut crate::blpapi_Name_t => name as *mut crate::blpapi_Name_t;
);

ffi_stub!(
    /// Find a Name by string (mock: return NULL - name not found)
    fn blpapi_Name_findName(_name_string: *const c_char) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

// ============================================================================
// Element function stubs
// ============================================================================

ffi_stub!(
    /// Get the Name of an Element (mock: return NULL)
    fn blpapi_Element_name(_element: *mut crate::blpapi_Element_t) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

// ============================================================================
// Message function stubs
// ============================================================================

ffi_stub!(
    /// Get the message type as a Name (mock: return NULL)
    fn blpapi_Message_messageType(
        _message: *mut crate::blpapi_Message_t,
    ) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

ffi_stub!(
    /// Get number of correlation IDs on a message (mock: return 1 for testing)
    fn blpapi_Message_numCorrelationIds(_message: *mut crate::blpapi_Message_t) -> usize => 1;
);

ffi_stub!(
    /// Get correlation ID at index (mock: return a zeroed correlation ID)
    ///
    /// Real Bloomberg signature (blpapi_message.h):
    ///   blpapi_CorrelationId_t blpapi_Message_correlationId(
    ///       const blpapi_Message_t *message, size_t index);
    fn blpapi_Message_correlationId(
        _message: *const crate::blpapi_Message_t,
        _index: usize,
    ) -> crate::blpapi_CorrelationId_t => unsafe { std::mem::zeroed() };
);

ffi_stub!(
    /// Get topic name for subscription messages (mock: return NULL)
    fn blpapi_Message_topicName(_message: *mut crate::blpapi_Message_t) -> *const c_char => ptr::null();
);

// Schema/Introspection stubs (return NULL/error)
// ============================================================================

ffi_stub!(
    /// SchemaElementDefinition name - returns blpapi_Name_t* (null in mock)
    fn blpapi_SchemaElementDefinition_name(_def: *const c_void) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_SchemaElementDefinition_description(_def: *const c_void) -> *const c_char => ptr::null();
);

ffi_stub!(
    fn blpapi_SchemaElementDefinition_type(_def: *const c_void) -> *mut c_void => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_SchemaElementDefinition_minValues(_def: *const c_void) -> usize => 0;
);

ffi_stub!(
    fn blpapi_SchemaElementDefinition_maxValues(_def: *const c_void) -> usize => 1;
);

ffi_stub!(
    /// SchemaTypeDefinition name - returns blpapi_Name_t* (null in mock)
    fn blpapi_SchemaTypeDefinition_name(_def: *const c_void) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_description(_def: *const c_void) -> *const c_char => ptr::null();
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_datatype(_def: *const c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_isComplexType(_def: *const c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_isSimpleType(_def: *const c_void) -> i32 => 1;
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_isEnumerationType(_def: *const c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_numElementDefinitions(_def: *const c_void) -> usize => 0;
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_getElementDefinitionAt(
        _def: *const c_void,
        _index: usize,
    ) -> *mut c_void => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_SchemaTypeDefinition_enumeration(_def: *const c_void) -> *mut c_void => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_Operation_name(_op: *const c_void) -> *const c_char => ptr::null();
);

ffi_stub!(
    fn blpapi_Operation_description(_op: *const c_void) -> *const c_char => ptr::null();
);

ffi_stub!(
    fn blpapi_Operation_requestDefinition(_op: *mut c_void, _def: *mut *mut c_void) -> i32 => -1;
);

ffi_stub!(
    fn blpapi_Operation_numResponseDefinitions(_op: *mut c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_Operation_responseDefinition(
        _op: *mut c_void,
        _def: *mut *mut c_void,
        _index: usize,
    ) -> i32 => -1;
);

// ConstantList/Constant stubs
ffi_stub!(
    fn blpapi_ConstantList_numConstants(_list: *const c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_ConstantList_getConstantAt(_list: *const c_void, _index: usize) -> *mut c_void => ptr::null_mut();
);

ffi_stub!(
    /// Constant name - returns blpapi_Name_t* (null in mock)
    fn blpapi_Constant_name(_constant: *const c_void) -> *mut crate::blpapi_Name_t => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_Constant_description(_constant: *const c_void) -> *const c_char => ptr::null();
);

// Service schema stubs
ffi_stub!(
    fn blpapi_Service_numOperations(_service: *mut c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_Service_description(_service: *mut c_void) -> *const c_char => ptr::null();
);

ffi_stub!(
    fn blpapi_Service_getOperationAt(
        _service: *mut c_void,
        _op: *mut *mut c_void,
        _index: usize,
    ) -> i32 => -1;
);

// ============================================================================
// Logging stubs (no-op)
// ============================================================================

ffi_stub!(
    fn blpapi_Logging_registerCallback(_callback: *const c_void, _user_data: *mut c_void) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_Logging_setLogLevel(_level: i32) -> i32 => 0;
);

// ============================================================================
// Identity/Auth stubs (return dummy handle or success)
// ============================================================================

ffi_stub!(
    fn blpapi_Session_createIdentity(
        _session: *mut crate::blpapi_Session_t,
    ) -> *mut blpapi_Identity_t => 1 as *mut blpapi_Identity_t;
);

ffi_stub!(
    fn blpapi_Session_generateAuthorizedIdentity(
        _session: *mut c_void,
        _auth_options: *const c_void,
        _correlation_id: *mut c_void,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_Identity_release(_identity: *mut c_void) => ();
);

// ============================================================================
// Request Templates stub (return NULL)
// ============================================================================

ffi_stub!(
    fn blpapi_Session_createSnapshotRequestTemplate(
        _session: *mut c_void,
        _subscription_string: *const c_char,
        _identity: *mut c_void,
        _correlation_id: *mut c_void,
    ) -> *mut c_void => ptr::null_mut();
);

ffi_stub!(
    fn blpapi_RequestTemplate_destroy(_template: *mut c_void) => ();
);

// ============================================================================
// Advanced SessionOptions stubs (no-op setters)
// ============================================================================

ffi_stub!(
    fn blpapi_SessionOptions_setMaxEventQueueSize(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _size: usize,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setSlowConsumerWarningHiWaterMark(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _mark: f32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setSlowConsumerWarningLoWaterMark(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _mark: f32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setDefaultKeepAliveInactivityTime(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _seconds: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setDefaultKeepAliveResponseTimeout(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _seconds: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setDefaultSubscriptionService(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _service: *const c_char,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setDefaultTopicPrefix(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _prefix: *const c_char,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setRecordSubscriptionDataReceiveTimes(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _record: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setConnectTimeout(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _timeout_ms: u32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setServiceCheckTimeout(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _timeout_ms: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setServiceDownloadTimeout(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _timeout_ms: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_maxEventQueueSize(
        _opts: *mut crate::blpapi_SessionOptions_t,
    ) -> usize => 10000;
);

ffi_stub!(
    fn blpapi_SessionOptions_setKeepAliveEnabled(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _enabled: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setBandwidthSaveModeDisabled(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _disabled: i32,
    ) -> i32 => 0;
);

ffi_stub!(
    fn blpapi_SessionOptions_setFlushPublishedEventsTimeout(
        _opts: *mut crate::blpapi_SessionOptions_t,
        _timeout_ms: i32,
    ) -> i32 => 0;
);
