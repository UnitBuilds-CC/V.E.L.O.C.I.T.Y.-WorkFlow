//! velocity-workflow-core
//! Hardware-native zero-allocation durable execution slab engine and C-ABI FFI core.

pub mod bitmask;
pub mod crdt;
pub mod ffi;
pub mod slab;

pub use bitmask::Bitmask256;
pub use crdt::PNCounter;
pub use ffi::*;
pub use slab::SlabHeader;
