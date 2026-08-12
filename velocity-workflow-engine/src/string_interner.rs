//! String interner for zero-allocation string handling on hot paths.
//!
//! Strings are interned once at system boundaries (FFI entry, API handlers)
//! and passed as `InternedString` (a `u32` index) through all hot paths.
//! Comparison of two interned strings is a single integer compare.
//!
//! The interner uses a `HashMap<String, u32>` for O(1) dedup. The key insight:
//! `String: Borrow<str>`, so `HashMap<String, V>::get(&str)` works without
//! allocating a `String` for the lookup.

use std::collections::HashMap;
use std::fmt;

/// A reference to an interned string. Just a `u32` index — 4 bytes, `Copy`,
/// `Eq` is integer comparison. Use `Interner::resolve()` to get the `&str`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InternedString(pub u32);

impl InternedString {
    /// Sentinel value for "no string".
    pub const EMPTY: InternedString = InternedString(u32::MAX);

    pub fn is_empty(self) -> bool {
        self.0 == u32::MAX
    }
}

impl fmt::Debug for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InternedString({})", self.0)
    }
}

impl Default for InternedString {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// String interner. Pre-allocates storage for interned strings.
/// Lookup is O(1) via HashMap. The HashMap is the single allocation —
/// after that, each unique string is stored once.
pub struct StringInterner {
    /// Stored strings. Index = InternedString.0
    strings: Vec<String>,
    /// Dedup map: string → index. Uses String: Borrow<str> for zero-alloc lookup.
    lookup: HashMap<String, u32>,
}

impl StringInterner {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            strings: Vec::with_capacity(capacity),
            lookup: HashMap::with_capacity(capacity),
        }
    }

    /// Intern a string. If already interned, returns the existing index
    /// **without any allocation**. Only allocates for genuinely new strings.
    pub fn intern(&mut self, s: &str) -> InternedString {
        // Zero-alloc lookup: String: Borrow<str> means this works
        if let Some(&idx) = self.lookup.get(s) {
            return InternedString(idx);
        }
        let idx = self.strings.len() as u32;
        self.strings.push(s.to_owned());
        self.lookup.insert(s.to_owned(), idx);
        InternedString(idx)
    }

    /// Resolve an interned string back to &str. O(1) index lookup.
    pub fn resolve(&self, interned: InternedString) -> &str {
        if interned.is_empty() {
            ""
        } else {
            &self.strings[interned.0 as usize]
        }
    }

    /// Number of unique interned strings.
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

/// Pre-interned common strings for the engine. Avoids even the HashMap lookup
/// for the hottest strings (activity types, signal names, etc).
pub struct InternedNames {
    pub workflow_started: InternedString,
    pub workflow_completed: InternedString,
    pub workflow_failed: InternedString,
    pub activity_scheduled: InternedString,
    pub activity_completed: InternedString,
    pub timer_started: InternedString,
    pub timer_fired: InternedString,
    pub signal_received: InternedString,
    pub query_received: InternedString,
    pub update_received: InternedString,
}

impl InternedNames {
    pub fn new(interner: &mut StringInterner) -> Self {
        Self {
            workflow_started: interner.intern("workflow_started"),
            workflow_completed: interner.intern("workflow_completed"),
            workflow_failed: interner.intern("workflow_failed"),
            activity_scheduled: interner.intern("activity_scheduled"),
            activity_completed: interner.intern("activity_completed"),
            timer_started: interner.intern("timer_started"),
            timer_fired: interner.intern("timer_fired"),
            signal_received: interner.intern("signal_received"),
            query_received: interner.intern("query_received"),
            update_received: interner.intern("update_received"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_dedup() {
        let mut interner = StringInterner::with_capacity(16);
        let a = interner.intern("hello");
        let b = interner.intern("hello");
        let c = interner.intern("world");

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_resolve() {
        let mut interner = StringInterner::with_capacity(16);
        let s = interner.intern("test_string");
        assert_eq!(interner.resolve(s), "test_string");
    }

    #[test]
    fn test_empty_sentinel() {
        let interner = StringInterner::with_capacity(4);
        assert_eq!(interner.resolve(InternedString::EMPTY), "");
        assert!(InternedString::EMPTY.is_empty());
    }

    #[test]
    fn test_zero_alloc_lookup() {
        let mut interner = StringInterner::with_capacity(16);
        let _ = interner.intern("first");
        let _idx = interner.intern("second");

        // Looking up "first" again should NOT allocate — just HashMap lookup by &str
        let len_before = interner.strings.len();
        let again = interner.intern("first");
        assert_eq!(again.0, 0); // Same index as first intern
        assert_eq!(interner.strings.len(), len_before); // No new allocation

        // Different string does allocate
        let _new = interner.intern("third");
        assert_eq!(interner.strings.len(), len_before + 1);
    }

    #[test]
    fn test_interned_names() {
        let mut interner = StringInterner::with_capacity(32);
        let names = InternedNames::new(&mut interner);
        assert_ne!(names.workflow_started, names.workflow_completed);
        assert_eq!(interner.resolve(names.workflow_started), "workflow_started");
    }

    #[test]
    fn test_comparison_is_integer() {
        let mut interner = StringInterner::with_capacity(16);
        let a = interner.intern("alpha");
        let b = interner.intern("beta");
        // Comparison should be just integer comparison
        assert!(a < b);
        assert_eq!(a, a);
    }
}
