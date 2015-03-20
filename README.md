# LlamaDB

**Warning**: This project is in the design/implementation phase, and is not
functional. Do NOT use this for anything you depend on!

LlamaDB is a versatile, speedy and low-footprint SQL database, written entirely
in the Rust programming language.

# Another SQL database? Why?

The project is driven by two personal goals:

1. Understand SQL better: both the language and the implementation details
2. Write a mission-critical project with Rust

# Example

Note: DOESN'T WORK YET. Intended design.

```rust
let mut db = llamadb::MemoryDatabase::new();

sql!(db,
    "CREATE TABLE account (
        id          UUID PRIMARY KEY,
        username    VARCHAR,
        password    BCRYPT
    )"
).unwrap();

let row = sql!(db,
    "INSERT INTO account(username, password) VALUES(?, ?)",
    "John", bcrypt("secretpassword")
).unwrap();

println!("{}", row["id"].getString());
```

## Principles

1. Keep it simple, stupid!
 * No triggers, no users, no embedded SQL languages (e.g. PL/SQL).
2. No temporary files
 * Temporary files can create unknown security violations if the user is unaware
   of them. For instance, SQLite will create temporary files on the OS's /tmp
   directory and in the same path as the SQLite database file.
 * The user may expect all database files to be on an encrypted drive.
   Sensitive data shouldn't leak to unexpected places.


## API design

When using the Rust or C API, all columns are sent and received as _byte arrays_.
It is up to the client-side driver to convert these types to numbers or strings if applicable.

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
