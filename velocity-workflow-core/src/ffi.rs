//! C-ABI Foreign Function Interface (FFI) bindings for zero-allocation C#/Rust interop.

use crate::arena::BumpArenaPage;
use crate::crdt::PNCounter;
use crate::nda::NdaHeader;
use crate::slab::SlabHeader;

#[no_mangle]
pub unsafe extern "C" fn velocity_slab_create(
    workflow_id: u64,
    run_id: u64,
    total_steps: u32,
    out_header: *mut SlabHeader,
) -> i32 {
    if out_header.is_null() {
        return -1; // Invalid null pointer
    }
    *out_header = SlabHeader::new(workflow_id, run_id, total_steps);
    0 // Success
}

#[no_mangle]
pub unsafe extern "C" fn velocity_slab_mark_step(
    header: *mut SlabHeader,
    step_index: u32,
) -> i32 {
    if header.is_null() {
        return -1;
    }
    if (*header).mark_step_completed(step_index as usize) {
        0
    } else {
        -2 // Out of bounds step index
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_slab_verify(header: *const SlabHeader) -> i32 {
    if header.is_null() {
        return -1;
    }
    if (*header).verify_merkle_root() {
        1 // Valid
    } else {
        0 // Invalid / Corrupted
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_slab_merge_crdt(
    target: *mut PNCounter,
    source: *const PNCounter,
) -> i32 {
    if target.is_null() || source.is_null() {
        return -1;
    }
    (*target).merge(&*source);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_nda_verify(header: *const NdaHeader) -> i32 {
    if header.is_null() {
        return -1;
    }
    if (*header).verify_merkle() {
        1
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_arena_alloc(
    arena: *mut BumpArenaPage,
    payload_ptr: *const u8,
    payload_len: usize,
    out_offset: *mut usize,
) -> i32 {
    if arena.is_null() || payload_ptr.is_null() || out_offset.is_null() {
        return -1;
    }
    let slice = std::slice::from_raw_parts(payload_ptr, payload_len);
    if let Some(offset) = (*arena).alloc_slice(slice) {
        *out_offset = offset;
        0
    } else {
        -2 // Page full
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_create_and_verify() {
        let mut header = SlabHeader::new(0, 0, 0);
        unsafe {
            let res = velocity_slab_create(777, 888, 5, &mut header);
            assert_eq!(res, 0);
            assert_eq!(header.workflow_id, 777);

            let valid = velocity_slab_verify(&header);
            assert_eq!(valid, 1);

            let step_res = velocity_slab_mark_step(&mut header, 2);
            assert_eq!(step_res, 0);
            assert_eq!(velocity_slab_verify(&header), 1);
        }
    }

    #[test]
    fn test_ffi_nda_verify() {
        let header = NdaHeader::new(5, 2, 128);
        unsafe {
            assert_eq!(velocity_nda_verify(&header), 1);
        }
    }
}
