# Column Types

## Primitive data types

LlamaDB's column types are designed to be orthogonal and non-ambiguous in their
use.

Primitive data types are the building blocks on which all LlamaDB column types
are built.

Primitive data types are meant to convey what is literally stored in the
database, not how said data is serialized or deserialized.

* `byte` - An octet, contains 8 bits of data.
* `T[]` - A variable-length array of types.
 * The equivalent of `BLOB` from MySQL would be `byte[]`
* `T[N]` - A fixed-length array of types, where `N` is a constant, positive integer.
 * The equivalent of `BINARY(N)` from MySQL would be `byte[N]`

### Dynamic sized bytes

All `byte[]` types have their lengths stored at the end of the key.

The length is stored as a 64-bit integer.

```sql
CREATE TABLE person(
    name        STRING,     -- backed by byte[]
    age         U8,
    description STRING      -- backed by byte[]
);
```

Key layout:
```
rowid: 8 bytes
name: 1 or more bytes (includes null terminator)
age: 1 byte
description: 1 or more bytes (includes null terminator)
name.length: 8 bytes
description.length: 8 bytes
```


## Abstract types

Abstract types are typed wrappers for `byte[]` or `byte[N]` primitives.

Abstract types address the following use cases:

* Input validation
* Serialization and deserialization for the SQL language

Types:

* `uX` - Unsigned integer
 * Backing primitive: `byte[Y]`, where `Y` = ceiling(`X` / 8)
* `iX` - Signed integer
 * Backing primitive: `byte[Y]`, where `Y` = ceiling(`X` / 8)
 * Range is two's complement. e.g. (`u8` has a range of -128 to +127)
* `f32` - A floating point number
 * Backing primitive: `byte[4]`
* `char` - A Unicode character; a code point
 * Backing primitive: `byte[4]`
* `string` - A UTF-8 encoded string.
 * Backing primitive: `byte[]`
 * The specified length (if any) is the maximum character length, not the byte length.
* `json` - Serialized JSON, useful for document stores as seen in NoSQL databases.
 * Backing primitive: `byte[]`
 * Validated and serialized using MessagePack
 * Canonical serialization, can be compared for equality
* `bool` - A boolean
 * Backing primitive: `byte[1]`
 * `true` on 1, `false` on 0
* `uuid` - A universally unique identifier.
 * Backing primitive: `byte[16]`
* `bcrypt` - A Bcrypt hash
 * Backing primitive: `byte[40]`
 * Serialized using Bcrypt BMCF Definition: https://github.com/ademarre/binary-mcf#bcrypt-bmcf-definition
 * 8-bit header, 128-bit salt, 184-bit digest (NOT 192-bit digest!)
  * If a 192-bit digest is provided, the last 8 bits will be discarded.
    This is due to a bug in the original bcrypt implementation that discards the
    last 8 bits on stringification.
 * The database simply stores bcrypt hashes; it cannot perform any hashing algorithms.
* `scrypt`
* `pbkdf2`

Aliases:

* `int` = `i32`
* `integer` = `i32`
* `float` = `f32`
* `varchar` = `string`

Any `byte[]` or `byte[N]` column can be converted to alternative representations:

```sql
SELECT bytes_column from MY_TABLE;
# {DE AD BE EF} (byte[])

SELECT bytes_column.hex from MY_TABLE;
# 'DEADBEEF' (varchar)

SELECT bytes_column.base64 from MY_TABLE;
# '3q2+7w==' (varchar)

INSERT INTO bytes_column VALUES ({DE AD C0 DE});

INSERT INTO bytes_column.hex VALUES ('DEADC0DE');
```

All abstract types have accessors to the backing primitive:

```sql
SELECT json_column from MY_TABLE;
# { "is_json": true } (json)

SELECT json_column.raw from MY_TABLE;
# {81 A7 69 73 5F 6A 73 6F 6E C3} (byte[])

SELECT json_column.raw.hex from MY_TABLE;
# '81A769735F6A736F6EC3' (varchar)

SELECT json_column.raw.base64 from MY_TABLE;
# 'gadpc19qc29uww==' (varchar)
```




## Nullable types

If a type is nullable, it requires an additional byte.
