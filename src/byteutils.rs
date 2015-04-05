pub fn read_u16_le(buf: &[u8]) -> u16 {
    assert_eq!(buf.len(), 2);

    buf.iter().enumerate().fold(0, |prev, (i, v)| {
        prev | ((*v as u16) << (i*8))
    })
}

pub fn read_u32_le(buf: &[u8]) -> u32 {
    assert_eq!(buf.len(), 4);

    buf.iter().enumerate().fold(0, |prev, (i, v)| {
        prev | ((*v as u32) << (i*8))
    })
}

pub fn read_u64_le(buf: &[u8]) -> u64 {
    assert_eq!(buf.len(), 8);

    buf.iter().enumerate().fold(0, |prev, (i, v)| {
        prev | ((*v as u64) << (i*8))
    })
}

#[must_use = "must use returned length"]
pub fn read_uvar(buf: &[u8]) -> Option<(usize, u64)> {
    let mut accum = 0;
    for (i, v) in buf.iter().enumerate() {
        let has_more = (v & 0x80) != 0;

        accum = (accum << 7) | (*v as u64 & 0x7F);

        if !has_more {
            return Some((i+1, accum));
        }
    }

    None
}

pub fn write_u16_le(value: u16, buf: &mut [u8]) {
    assert_eq!(buf.len(), 2);

    for (i, v) in buf.iter_mut().enumerate() {
        let byte = ((value & (0xFF << (i*8))) >> (i*8)) as u8;
        *v = byte;
    }
}

pub fn write_u32_le(value: u32, buf: &mut [u8]) {
    assert_eq!(buf.len(), 4);

    for (i, v) in buf.iter_mut().enumerate() {
        let byte = ((value & (0xFF << (i*8))) >> (i*8)) as u8;
        *v = byte;
    }
}

pub fn write_u64_le(value: u64, buf: &mut [u8]) {
    assert_eq!(buf.len(), 8);

    for (i, v) in buf.iter_mut().enumerate() {
        let byte = ((value & (0xFF << (i*8))) >> (i*8)) as u8;
        *v = byte;
    }
}

pub fn read_udbinteger(bytes: &[u8]) -> u64 {
    assert!(bytes.len() >= 1 && bytes.len() <= 8);

    bytes.iter().fold(0, |prev, &v| {
        (prev << 8) | (v as u64)
    })
}

pub fn read_sdbinteger(bytes: &[u8]) -> i64 {
    use std::mem;
    use std::num::Wrapping;

    let value = read_udbinteger(bytes);

    let n = bytes.len() as u64;
    let half_unsigned_maximum: i64 = 1 << (n * 8 - 1);
    unsafe {
        // assumes that the platform uses two's complement
        // use wrapping to prevent overflow panics at runtime
        let v: i64 = mem::transmute(value);
        (Wrapping(v) - Wrapping(half_unsigned_maximum)).0
    }
}

pub fn read_dbfloat(bytes: &[u8]) -> f64 {
    use std::mem;

    assert_eq!(bytes.len(), 8);

    let raw = read_udbinteger(bytes);

    unsafe {
        if bytes[0] < 0x80 {
            mem::transmute(raw ^ (!0))
        } else {
            mem::transmute(raw ^ (1 << 63))
        }
    }
}

pub fn write_udbinteger(value: u64, buf: &mut [u8]) {
    assert!(buf.len() >= 1 && buf.len() <= 8);
    let len = buf.len();

    for (i, v) in buf.iter_mut().enumerate() {
        let j = len - 1 - i;
        let byte = ((value & (0xFF << (j*8))) >> (j*8)) as u8;
        *v = byte;
    }
}

pub fn write_sdbinteger(value: i64, buf: &mut [u8]) {
    use std::mem;

    let n = buf.len() as u64;
    let half_unsigned_maximum: u64 = 1 << (n * 8 - 1);

    let r: u64 = unsafe {
        // assumes that the platform uses two's complement
        let unsigned: u64 = mem::transmute(value);
        unsigned ^ half_unsigned_maximum
    };

    write_udbinteger(r, buf)
}

pub fn write_dbfloat(value: f64, buf: &mut [u8]) {
    use std::mem;

    let raw: u64 = unsafe {
        // assumes that the platform uses IEEE 754 encoding for floats
        mem::transmute(value)
    };

    if value < 0.0 {
        write_udbinteger(raw ^ (!0), buf)
    } else {
        write_udbinteger(raw ^ (1 << 63), buf)
    }
}

/// Maximum buffer size needed for 64-bit number: 10 bytes
#[must_use = "must use returned length"]
pub fn write_uvar(value: u64, buf: &mut [u8]) -> Option<usize> {
    let mut remainder = value;

    for i in 0..buf.len() {
        let data = (remainder & 0x7F) as u8;
        remainder = remainder >> 7;
        let has_more = remainder != 0;

        buf[i] = if i == 0 {
            data
        } else {
            0x80 | data
        };

        if !has_more {
            // Reverse the buffer; most significant numbers should go first.
            buf[0..i+1].reverse();
            return Some(i + 1)
        }
    }

    // The buffer wasn't long enough
    None
}

#[cfg(test)]
mod test {
    use super::{read_u16_le, read_u32_le, read_u64_le, read_uvar};
    use super::{read_udbinteger, read_sdbinteger};
    use super::{write_u16_le, write_u32_le, write_u64_le, write_uvar};
    use super::{write_udbinteger, write_sdbinteger};
    use std;

    static TEST_U16: [(u16, &'static [u8]); 3] = [
        (0x0201, &[0x01, 0x02]),
        (0x0000, &[0x00, 0x00]),
        (0xFFFF, &[0xFF, 0xFF]),
    ];

    static TEST_U32: [(u32, &'static [u8]); 1] = [
        (0x04030201, &[0x01, 0x02, 0x03, 0x04])
    ];

    static TEST_U64: [(u64, &'static [u8]); 1] = [
        (0x0807060504030201, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    ];

    static TEST_UVAR: [(u64, &'static [u8]); 8] = [
        (0x00, &[0x00]),
        (0x7F, &[0x7F]),
        (0x80, &[0x81, 0x00]),
        (0xFF, &[0x81, 0x7F]),
        (0x0100, &[0x82, 0x00]),
        (0xFFFF_FFFF, &[0x8F, 0xFF, 0xFF, 0xFF, 0x7F]),
        (0x1234_5678_9ABC_DEF0, &[0x92, 0x9A, 0x95, 0xCF, 0x89, 0xD5, 0xF3, 0xBD, 0x70]),
        (0xFFFF_FFFF_FFFF_FFFF, &[0x81, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
    ];

    static TEST_UDB: [(u64, &'static [u8]); 5] = [
        (0, &[0x00]),
        (255, &[0x00, 0x00, 0x00, 0xFF]),
        (0, &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (1 << 63, &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (!0, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
    ];

    static TEST_SDB: [(i64, &'static [u8]); 7] = [
        (-1, &[0x7F]),
        (-32768, &[0x00, 0x00]),
        (-1, &[0x7F, 0xFF]),
        (0, &[0x80, 0x00]),
        (32767, &[0xFF, 0xFF]),
        (std::i64::MIN, &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (std::i64::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
    ];

    #[test]
    fn test_read_u16_le() {
        for &(v, buf) in TEST_U16.iter() {
            assert_eq!(v, read_u16_le(buf));
        }
    }

    #[test]
    fn test_read_u32_le() {
        for &(v, buf) in TEST_U32.iter() {
            assert_eq!(v, read_u32_le(buf));
        }
    }

    #[test]
    fn test_read_u64_le() {
        for &(v, buf) in TEST_U64.iter() {
            assert_eq!(v, read_u64_le(buf));
        }
    }

    #[test]
    fn test_read_uvar() {
        for &(v, buf) in TEST_UVAR.iter() {
            assert_eq!((buf.len(), v), read_uvar(buf).unwrap());
        }
    }

    #[test]
    fn test_read_udbinteger() {
        for &(v, buf) in TEST_UDB.iter() {
            assert_eq!(v, read_udbinteger(buf));
        }
    }

    #[test]
    fn test_read_sdbinteger() {
        for &(v, buf) in TEST_SDB.iter() {
            assert_eq!(v, read_sdbinteger(buf));
        }
    }

    #[test]
    fn test_write_u16_le() {
        let mut write_buf = [0; 2];

        for &(v, buf) in TEST_U16.iter() {
            write_u16_le(v, &mut write_buf);
            assert_eq!(buf, write_buf);
        }
    }

    #[test]
    fn test_write_u32_le() {
        let mut write_buf = [0; 4];

        for &(v, buf) in TEST_U32.iter() {
            write_u32_le(v, &mut write_buf);
            assert_eq!(buf, write_buf);
        }
    }

    #[test]
    fn test_write_u64_le() {
        let mut write_buf = [0; 8];

        for &(v, buf) in TEST_U64.iter() {
            write_u64_le(v, &mut write_buf);
            assert_eq!(buf, write_buf);
        }
    }

    #[test]
    fn test_write_uvar() {
        let mut write_buf = [0; 10];

        for &(v, buf) in TEST_UVAR.iter() {
            let written = write_uvar(v, &mut write_buf).unwrap();
            assert_eq!(buf, &write_buf[0..written]);
        }
    }

    #[test]
    fn test_write_udbinteger() {
        let mut write_buf = [0; 8];

        for &(v, buf) in TEST_UDB.iter() {
            let mut b = &mut write_buf[0..buf.len()];
            write_udbinteger(v, b);
            assert_eq!(buf, b);
        }
    }

    #[test]
    fn test_write_sdbinteger() {
        let mut write_buf = [0; 8];

        for &(v, buf) in TEST_SDB.iter() {
            let mut b = &mut write_buf[0..buf.len()];
            write_sdbinteger(v, b);
            assert_eq!(buf, b);
        }
    }
}
