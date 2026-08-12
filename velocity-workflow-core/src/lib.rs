//! velocity-workflow-core
//! Hardware-native zero-allocation durable execution slab engine and C-ABI FFI core.

pub mod arena;
pub mod bitmask;
pub mod crdt;
pub mod ffi;
pub mod nda;
pub mod slab;
pub mod vctp;
pub mod wal;

pub use arena::BumpArenaPage;
pub use bitmask::Bitmask256;
pub use crdt::PNCounter;
pub use ffi::*;
pub use nda::NdaHeader;
pub use slab::SlabHeader;
pub use vctp::{AimdController, VctpPacketHeader};
pub use wal::wal_append_step;
