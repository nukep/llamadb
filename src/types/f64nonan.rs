use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Deref;

/// A double-precision floating point number, `f64`, guaranteed never to be NaN.
#[derive(Copy, Clone, PartialOrd, PartialEq)]
pub struct F64NoNaN {
    value: f64
}

impl F64NoNaN {
    pub fn new(value: f64) -> Option<F64NoNaN> {
        if value.is_nan() {
            None
        } else {
            Some(F64NoNaN { value: value })
        }
    }
}

impl Deref for F64NoNaN {
    type Target = f64;

    fn deref(&self) -> &f64 { &self.value }
}

impl Eq for F64NoNaN { }
impl Ord for F64NoNaN {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for F64NoNaN {
    fn hash<H>(&self, state: &mut H) where H: Hasher {
        let raw: u64 = unsafe { mem::transmute(self.value) };
        raw.hash(state)
    }
}
