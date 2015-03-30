# LlamaDB

**Warning**: This project is in the design/implementation phase, and is not
stable.
Do NOT use this for launching rockets or for anything that's important to you!

LlamaDB is a versatile, speedy and low-footprint SQL database, written entirely
in the Rust programming language.

# Another SQL database? Why?

The project is driven by two goals:

1. The author wants to understand SQL better: both the language and the implementation details.
2. Show what Rust can do for SQL, much like what Servo is doing for web browsers.

# Example CLI usage

```
llamadb> CREATE TABLE person (
    ...>   name VARCHAR,
    ...>   age U32
    ...> );
Created.
llamadb> INSERT INTO person VALUES ('Bob', 24), ('Fred', 45);
2 rows inserted.
llamadb> SELECT * FROM person;
--------------
| name | age |
--------------
| Bob  | 24  |
| Fred | 45  |
--------------

2 rows selected.
llamadb> SELECT name || ' is ' || age || ' years old.' AS message FROM person;
-------------------------
| message               |
-------------------------
| Bob is 24 years old.  |
| Fred is 45 years old. |
-------------------------
llamadb> EXPLAIN SELECT * FROM person WHERE age >= 18;
query plan
column names: (`name`, `age`)
(scan `person` :source_id 0
  (if
    (>=
      (column-field :source_id 0 :column_offset 1)
      18)
    (yield
      (column-field :source_id 0 :column_offset 0)
      (column-field :source_id 0 :column_offset 1))))
llamadb>
```

## Features and TODO

* [x] Command-line interface.
* [ ] Stable C API.
* [x] `CREATE TABLE`, `SELECT`.
* [x] `EXPLAIN` - show the query plan for a `SELECT` statement.
* [x] `INSERT`.
* [ ] `UPDATE`, `DELETE`.
* [ ] Table indices, `CREATE INDEX`.
* [x] Nested and correlated subqueries.
* [ ] `GROUP BY`, `HAVING`, `ORDER BY`.
* [ ] `LIMIT`.
* [ ] `INSERT` using a `SELECT` query.
* [x] Implicit cross joining, eg. `...FROM table1, table2...`.
* [ ] Inner and outer joins (use WHERE for inner joins for now).
* [ ] Column constraints.
* [ ] Auto-incrementing column.
* [ ] Persistent disk database (B+Tree and pager need more work!).
* [ ] Transactions, locking, ACID.

## Data types

* **`STRING` / `VARCHAR`**
 * A variable-length UTF-8 string.
* **`Ux`**, where x is >= 8 and <= 64, and is a multiple 8.
 * An unsigned integer.
* **`Sx`**, where x is >= 8 and <= 64, and is a multiple 8.
 * An signed integer.
* **`F64` / `DOUBLE`**
 * A double-precision (64-bit) floating point number.
* **`byte[]`**
 * A variable-length byte array.
* **`byte[N]`**
 * A fixed-length byte array.


## Principles

1. Keep it simple, stupid!
 * No triggers, no users, no embedded SQL languages (e.g. PL/SQL).
2. No temporary files
 * Temporary files can create unknown security violations if the user is unaware
   of them. For instance, SQLite will create temporary files on the OS's /tmp
   directory and in the same path as the SQLite database file.
 * The user may expect all database files to be on an encrypted drive.
   Sensitive data shouldn't leak to unexpected places.


## Security principles

1. The database should NEVER receive plain-text passwords.
 * This is why LlamaDB does not include any hashing functions;
   the inclusion of hashing functions would encourage the sending of plain-text
   passwords over a database connection.
 * If the DBAs and developers aren't careful, the passwords could be logged by
   the DBMS through query logging.
 * Hashing algorithms such as bcrypt (should) require a CRNG for the salt, and
   embedding CRNGs is not in the scope of LlamaDB.
 * Nowadays, it's easy to perform application-side hashing. There's a vast
   supply of good crypto libraries for every important programming language.

## NULL

Unlike standard SQL, NULL is opt-in on table creation.
For users of other SQL databases, just think of all `CREATE TABLE` columns as
having an implicit `NOT NULL`.

Null still exists as a placeholder value and for outer joins; it's just not the
default for `CREATE TABLE` columns.
If NULL is desired for a column, add the `NULL` constraint.


## Special thanks

A **HUGE THANKS** goes out to SQLite and the SQLite documentation.
Their wonderful docs helped shed some light on the SQL syntax and other crucial
details such as their B-Tree implementation.
