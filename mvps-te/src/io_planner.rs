use std::ops::Range;

/// Translates linear address to (page_id, byte_range_in_page) tuples
pub struct IoPlannerIterator {
  page_size: u64,
  linear_end: u64,
  page_id: u64,
  offset_in_page: u64,
}

impl Iterator for IoPlannerIterator {
  type Item = (u64, Range<u64>);

  fn next(&mut self) -> Option<Self::Item> {
    let linear_current = self.page_id * self.page_size + self.offset_in_page;
    if linear_current >= self.linear_end {
      return None;
    }

    let page_id = self.page_id;
    let offset_in_page = self.offset_in_page;
    self.page_id += 1;
    self.offset_in_page = 0;

    let page_end = (page_id + 1) * self.page_size;
    Some(if page_end > self.linear_end {
      (
        page_id,
        offset_in_page..self.linear_end & (self.page_size - 1),
      )
    } else {
      (page_id, offset_in_page..self.page_size)
    })
  }
}

impl IoPlannerIterator {
  pub fn new(page_size: u64, linear_addr: Range<u64>) -> Self {
    Self {
      page_size,
      linear_end: linear_addr.end,
      page_id: linear_addr.start / page_size,
      offset_in_page: linear_addr.start % page_size,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_io_planner_iterator() {
    let page_size = 4096;

    let mut iter = IoPlannerIterator::new(page_size, 0..12288);

    assert_eq!(iter.next(), Some((0, 0..4096)));
    assert_eq!(iter.next(), Some((1, 0..4096)));
    assert_eq!(iter.next(), Some((2, 0..4096)));
    assert_eq!(iter.next(), None);

    let mut iter = IoPlannerIterator::new(page_size, 4095..12288);

    assert_eq!(iter.next(), Some((0, 4095..4096)));
    assert_eq!(iter.next(), Some((1, 0..4096)));
    assert_eq!(iter.next(), Some((2, 0..4096)));
    assert_eq!(iter.next(), None);

    let mut iter = IoPlannerIterator::new(page_size, 4096..12288);

    assert_eq!(iter.next(), Some((1, 0..4096)));
    assert_eq!(iter.next(), Some((2, 0..4096)));
    assert_eq!(iter.next(), None);

    let mut iter = IoPlannerIterator::new(page_size, 4100..12289);

    assert_eq!(iter.next(), Some((1, 4..4096)));
    assert_eq!(iter.next(), Some((2, 0..4096)));
    assert_eq!(iter.next(), Some((3, 0..1)));
    assert_eq!(iter.next(), None);

    let mut iter = IoPlannerIterator::new(page_size, 5000..5001);

    assert_eq!(iter.next(), Some((1, 904..905)));
    assert_eq!(iter.next(), None);
  }
}
