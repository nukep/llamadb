# B+Tree

The only index that LlamaDB supports so far is the B+Tree index.

The B+Tree is a data structure optimized for ranged searches, insertions
and deletions.

The B+Tree was chosen instead of the B-Tree because it supports direct traversal
over leaf nodes when iterating a range of keys.
For example, performing a `SELECT * FROM table;` will not repeatedly climb up
and down interior pages, as would happen with B-Trees.

Each B+Tree has a property for Cell length.
Any key in excess of this length has its remainder put into overflow pages.

Each B+Tree cell has a fixed-length.
The intent of using fixed-length cells instead of variable-length cells is to
simplify searches, node splitting and insertions.

While it's true that fixed-length cells may lead to wasted space, the real-world
problems that arise from this is likely insignificant.
As a general rule of thumb, LlamaDB values performance and simplicity over
saving a few bytes of disk space.


## B+Tree page structure

### Header, 16 bytes

| Size and type | Name                                        |
|---------------|---------------------------------------------|
| 1, u8-le      | Flags                                       |
| 3             | unused/reserved                             |
| 2, u16-le     | Page cell count                             |
| 2, u16-le     | Cell length                                 |
| 8, u64-le     | Right page pointer                          |

**Flags: 000000RL**

* R: Root page. 0 if non-root page, 1 if root page.
* L: Leaf page. 0 if interior page, 1 if leaf page.

For leaf nodes, the right page pointer is a reference to the next leaf node for traversal.
The referenced leaf node contains keys that are sorted after ones from the current leaf node.
All leaf nodes form a linked list that contain all keys in the B+Tree.

If the page is the last leaf page of the B+Tree, there is no right page pointer.
In this case, the right page pointer is set to a value of 0.
In all other cases, the right page pointer must be set to a valid page id.

Cell length is the fixed length of the Cell structure.
Cell length must be a minimum of 24 bytes: 20 byte header + minimum 4-byte in-page payload.

All child B+Tree pages must have the same cell length as the root B+Tree page.
This invariant is useful for node splitting: a cell can then simply be moved
byte-for-byte into a new page without worrying about incompatible cell lengths.


Let P = Page size. Let L = Cell length. Let C = Max page cell count.

* C = floor((P - 16) / L)
* L = floor((P - 16) / C)
* C has the minimum: 2
* C has the maximum: floor((P - 16) / 24, 2)
* L has the minimum: 24
* L has the maximum: (P - 16) / 2

| Page size | Cell length | Max cell count per page |
|-----------|-------------|-------------------------|
| 65536     | 24          | 2730                    |
| 65536     | 32760       | 2                       |
| 4096      | 24          | 170                     |
| 4096      | 2040        | 2                       |
| 512       | 24          | 20                      |
| 512       | 248         | 2                       |
| 64        | 24          | 2                       |

Note that 65536 is the largest allowed page size,
and 64 is the smallest allowed page size.

### Cell

| Size and type     | Name                                     |
|-------------------|------------------------------------------|
| 8, u64-le         | Left page pointer (ignored if leaf page) |
| 4, u32-le         | Payload length                           |
| 8, u64-le         | _Overflow page (omitted if not needed)_  |
| Remainder of cell | In-page payload                          |

The left page pointer is _ignored_ instead of _omitted_ for leaf pages.
This is to avoid issues in the event that a leaf page is converted to an
interior page.

Rationale:
If the left page pointer were omitted for leaf pages, the pointer would need to
be added back when the cell is converted for an interior page. The cell length
is always fixed, so in the event that the cell also has overflow data,
all of the overflow data _and all of its pages_ would need to be shifted
by 8 bytes.
The current solution doesn't need to read the overflow pages, which is better
for caching.

If the payload length is less than the remainder of the cell, the data is
padded with zeros.


## Insertion

TODO
