# Indexing and Sorting

This is perhaps the most important implementation problem that SQL databases
must address.

## Simple and ignorant

All sorting is done with simple `memcpy()` operations.
This means that all keys' byte representations sort the same way as the keys
do semantically.
The B+Tree traversal algorithm is kept simpler this way.

The algorithm doesn't need to be aware of the types contained in the keys, so
there's no need for specialized comparators.
To the traversal algorithm, all keys are simple byte collections that are always
ordered the same way.


## Byte sorting

All keys are stored and sorted as a collection of bytes.

Here's a sorted byte list:
```
00
00 00
00 00 FF
00 01
01
02 00
...
FE FF FF FF FF FF FF
FF
FF 00
FF FF
FF FF FF
FF FF FF FF
```

Keys that share the same beginning as another key but are longer are sorted after.


## Integers

All integer keys are stored as big-endian.
If the integer is signed, then add half of the unsigned maximum (8-bit => 128).

* 255 unsigned 4-byte => `00 00 00 FF`
* -32768 signed 2-byte => `00 00`
* -1 signed 2-byte => `7F FF`
* 0 signed 2-byte => `80 00`
* 32767 signed 2-byte => `FF FF`


## Strings

All string keys are stored as UTF-8 and are null-terminated.
A length is not prefixed because this would effectively make the strings sorted
by length instead of lexicographically.

UTF-8 has the property of lexicographic sorting. Even with extension bytes,
the string will sort in ascending order of the code points.

The null terminator is used to indicate the end of the string, as an
optimization to prevent reading the last page(s) for the length.
String is backed with `byte[]`, so the string length + 1 is stored at the end of
the key. When searching lexicographically, this is ignored.
It also serves as a separator from other multi-column values in the key.

Longer strings that share the same beginning as another string are sorted after.

```
41 70 70 6C 65 00               // Apple
41 70 70 6C 65 73 00            // Apples
41 CC 88 70 66 65 6C 00         // Äpfel (NFD)
42 61 6E 61 6E 61 00            // Banana
42 61 6E 61 6E 61 73 00         // Bananas
42 61 6E 64 00                  // Band
42 65 65 68 69 76 65 00         // Beehive
42 65 65 73 00                  // Bees
61 70 70 6C 65 00               // apple
C3 84 70 66 65 6C 00            // Äpfel (NFC)
```

* `WHERE x LIKE 'Apple%'` => `41 70 70 6C 65`
* `WHERE x = 'Apple'` => `41 70 70 6C 65 00`

Strings are sorted by their UTF-8 representation, and not with a collation
algorithm.
It's theoretically possible to index strings using a collation algorithm if
the algorithm can return a byte representation that sorts the same way.
However, this is not yet supported.


## Floating point numbers

This encoding is mostly compatible with the number ranges from IEEE 754.
The only exception is NaN, which this encoding does not support.

NaN is unsortable/imcomparable, and therefore cannot be encoded.

This encoding is basically the same as binary32 IEEE 754, but with flipped bits.
Like the integer types, the encoding is in big-endian
(the byte with the sign bit comes first).

To convert IEEE 754 to or from this encoding:

* If the number is negative, flip all the bits.
* If the number is positive, flip the sign bit.

This way, an encoding of `00 7F FF FF` is a negative number with the highest exponent and the highest mantissa,
which would be the smallest possible floating point number.
Similarly, an encoding of `FF 80 00 00` is a positive number with the highest exponent and the highest mantissa,
which would be the largest possible floating point number.

* -inf => `00 7F FF FF`
* -1 => `40 7F FF FF`
* -0 => `7F FF FF FF`
* +0 => `80 00 00 00`
* +1 => `BF 80 00 00`
* +inf => `FF 80 00 00`

The removal of NaN disqualifies 16,777,214 values.
Ranges that the removal of NaN disqualifies (inclusive):

* `00 00 00 00` to `00 7F FF FE`
* `FF 80 00 01` to `FF FF FF FF`
