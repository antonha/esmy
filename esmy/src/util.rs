use std::io::Error;
use std::io::Read;
use std::io::Write;

#[inline]
pub fn write_vint(write: &mut Write, mut value: u64) -> Result<u32, Error> {
    let mut count = 1;
    while (value & !0x7F) != 0 {
        write.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        value >>= 7;
        count += 1;
    }
    write.write_all(&[(value as u8)])?;
    Result::Ok(count)
}

#[inline]
pub fn read_vint(read: &mut Read) -> Result<u64, Error> {
    let mut buf = [1];
    read.read_exact(&mut buf)?;
    let mut res: u64 = u64::from(buf[0] & 0x7F);
    let mut shift = 7;
    while (buf[0] & 0x80) != 0 {
        read.read_exact(&mut buf)?;
        res |= u64::from(buf[0] & 0x7F) << shift;
        shift += 7
    }
    Ok(res as u64)
}

#[cfg(test)]
mod tests {

    use super::read_vint;
    use super::write_vint;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use std::io::Cursor;

    proptest!{
        #![proptest_config(Config::with_cases(100_000))]
        #[test]
        fn read_write_correct(num in any::<u64>()) {
            let mut write = Cursor::new(vec![0 as u8; 100]);
            write_vint(&mut write, num).unwrap();
            write.set_position(0);
            assert!(num == read_vint(&mut write).unwrap())
        }
    }
}
