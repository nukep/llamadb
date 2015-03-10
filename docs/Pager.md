# Pager

The pager module partitions a backing store into a cache-friendly, addressable,
and fixed-sized collection of pages.

## Pager implementations

There are two pager implementations: **Disk** and **Memory**.

* A disk pager ideally has a page size that matches the device's sector size.
 * This is usually 512 or 4096 bytes.
* A memory pager ideally has a page size that matches the CPU's cache line size.
 * On most architectures, this is 4096 bytes.

For the most part, the disk and memory pagers have a lot in common.
Both pagers' backing stores are segmented at the hardware level, and if
exploited can yield faster data access through caching.

This means the same pager abstractions can be used for both disk and memory
without the abstractions being too leaky.

## Invariants

* Page ID can be any unique value except for 0.
* The minimum page size: **64 bytes**.
* The maximum page size: **65536 bytes**.
* The page size must be a power of 2.
