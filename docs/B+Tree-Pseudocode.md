# Pseudocode

To be used as a reference for implementation.

## Key comparison

B+Tree key comparison is done in a way that avoids loading overflow pages.

```
# Compares a cell with a key. Returns <, =, or >
compare_cell(cell, key):
    v = cell.in_page_payload.truncate_to(key.length)

    # Compare the bytes of v and key

    return v.compare(key) if not Equal
    return Equal if cell.payload_length <= key.length

    # Sad path: we need to read overflow pages.

    for overflow_page in overflow pages:
        v = overflow_page.truncate_to(key.length - overflow_offset)
        return v.compare(key[overflow_offset..]) if not Equal

    # Every overflow page compared equal. The entire payload is equal.

    return Equal
```

## Key search

```
find_keys(root_page_id, min, max):
    (right_pointer, cells) = find_min_leaf_offset(root_page_id, min)

    loop:
        for cell in cells:
            if cell > max:
                return

            read full cell data
            yield cell data

        # End of leaf node. Keep traversing right.

        if right_pointer:
            page = read right pointer page
            right_pointer = parse page header.right_pointer
            cells = parse page cells
        else:
            return

find_min_leaf_offset(page_id, min):
    right_pointer = (parse page header).right_pointer
    cells = parse page cells

    if leaf:
        offset = offset where cell >= min
        if offset found:
            return (right_pointer, cells[offset..])
        else if right_pointer:
            # The offset is definitely the first cell on the right page.
            right_page_header = parse right pointer page header
            return (right_page_header.right_pointer, read right page cells)
        else:
            # No offset can be found
            return None
    else:
        for cell in cells:
            if cell <= min:
                return find_min_leaf_offset(cell.left_pointer)
        return find_min_leaf_offset(right_pointer)

```

## Key insertion
```
insert_key_to_root(root_page_id, key):
    if root page is leaf:
        insert_key_to_root_leaf(root_page_id, key)
    else:
        if let Some(insert_to_parent) = insert_key_to_nonroot(page_id, key):


insert_key_to_nonroot(page_id, key):
    if page is leaf:
        insert_key_to_nonroot_leaf(page_id, key)
    else:
        cells = parse page cells

insert_key_to_root_leaf(page_id, key):

insert_key_to_nonroot_leaf(page_id, key):
    cells = parse page cells
    insert key into cells

    if cells.length > MAX_CELLS:
        # Split the node

        # Get split offset. Uses truncating division.
        split_offset = cells.length / 2

        new_page_id = create new page
        copy cells[..split_offset] to page_id
        copy cells[split_offset..] to new_page_id

        new_page_id.right_pointer = page_id.right_pointer
        page_id.right_pointer = new_page_id

        # Tell the caller about the left and right page ids, and the split key
        return Some(page_id, new_page_id, cells[split_offset])
    else:
        copy cells to page_id
        return None
```
