use crate::slab::SlabHeader;

#[repr(C)]
pub struct WalEntry {
    pub sequence: u64,
    pub workflow_id: u64,
    pub step_index: u32,
    pub timestamp: u64,
}

pub fn wal_append_step(header: &mut SlabHeader, step_index: u32) -> Result<(), i32> {
    if step_index >= header.total_steps {
        return Err(-1);
    }
    if header.mark_step_completed(step_index as usize) {
        Ok(())
    } else {
        Err(-2)
    }
}
