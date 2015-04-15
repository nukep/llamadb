# Table of contents

* [CREATE TABLE](#create-table)
* [INSERT](#insert)
* [SELECT](#select)
* [EXPLAIN](#explain)

# CREATE TABLE

## Column data types

* **`STRING` / `VARCHAR`**
 * A variable-length UTF-8 string.
* **`Ux`**, where x is >= 8 and <= 64, and is a multiple 8.
 * An unsigned integer.
* **`Ix`**, where x is >= 8 and <= 64, and is a multiple 8.
 * An signed integer.
* **`F64` / `DOUBLE`**
 * A double-precision (64-bit) floating point number.
* **`byte[]`**
 * A variable-length byte array.
* **`byte[N]`**
 * A fixed-length byte array.

## NULL

Unlike standard SQL, `NULL` is opt-in on table creation.
For users of other SQL databases, just think of all `CREATE TABLE` columns as
having an implicit `NOT NULL`.

Null still exists as a placeholder value and for outer joins; it's just not the
default for `CREATE TABLE` columns.
If NULL is desired for a column, add the `NULL` constraint.

## Example

```sql
CREATE TABLE person (
    id U32,
    name STRING,
    age U8,
    country_id U32,
    salary U64 NULL     -- column is nullable; person may or may not be employed
);

CREATE TABLE county (
    id U32,
    name STRING,
    formation_year I16
);
```

Note: LlamaDB doesn't support primary keys or auto-incrementing columns yet!


# SELECT

LlamaDB supports much of `SELECT`, including `GROUP BY` and nested/correlated subqueries.

Missing `SELECT` features are, but not limited to:

* `INNER JOIN` and `OUTER JOIN` (for now, use `WHERE` for inner joins)
* `ORDER BY`
* `LIMIT`
* `DISTINCT`
* Unimplemented expressions in general, such as `CASE`, `EXISTS` and `IN`


# INSERT

## Example

```sql
INSERT INTO country VALUES
(0, 'Canada', 1867),
(1, 'United States of America', 1776);

INSERT INTO person VALUES
(0, 'Joe', 35, 0, NULL),
(1, 'Quentin', 61, 1, 44232),
(2, 'Barbara', 17, 1, NULL),
(2, 'Joanne', 26, 0, 51700);
```


## Example

Note: The `testdata` command runs [this script](cli/src/testdata.sql).

```sql
-- Loads the hard-coded "Chinook" test database.
-- Populates the tables: Album, Artist, Genre, MediaType, Track
testdata

SELECT title AS album, name AS artist
FROM album, artist
WHERE album.artistid = artist.artistid;
/*
----------------------------------------------------------------------------------
| album                                            | artist                      |
----------------------------------------------------------------------------------
| For Those About To Rock We Salute You            | AC/DC                       |
| Let There Be Rock                                | AC/DC                       |
| Balls to the Wall                                | Accept                      |
| Restless and Wild                                | Accept                      |
| Big Ones                                         | Aerosmith                   |
| Jagged Little Pill                               | Alanis Morissette           |
| Facelift                                         | Alice In Chains             |
| Warner 25 Anos                                   | Antônio Carlos Jobim        |
                             ... many more rows ...
347 rows selected.
*/

SELECT (
    SELECT genre.name FROM genre WHERE genre.genreid = track.genreid
) genre, count(*) num_tracks, avg(milliseconds) / 1000 avg_seconds
FROM track GROUP BY genreid;
/*
-------------------------------------------------
| genre              | num_tracks | avg_seconds |
-------------------------------------------------
| Blues              | 81         | 270.359778  |
| Electronica/Dance  | 30         | 302.9858    |
| Opera              | 1          | 174.813     |
| Comedy             | 17         | 1585.263706 |
| Rock               | 1297       | 283.910043  |
| R&B/Soul           | 61         | 220.066852  |
| World              | 28         | 224.923821  |
| TV Shows           | 93         | 2145.041022 |
| Metal              | 374        | 309.749444  |
| Alternative        | 40         | 264.058525  |
         ... many more rows ...
25 rows selected.
*/
```

# EXPLAIN

LlamaDB represents all query execution plans in a Lisp-style notation.
Basically, you get to see the _entire_ execution represented; there are no missing details.

To get the query execution plan for a query, prepend the query with the `EXPLAIN` keyword:

```sql
EXPLAIN SELECT name, age FROM person WHERE age >= 18;
```
```lisp
(scan `person` :source-id 0
  (if
    (>=
      (column-field :source-id 0 :column-offset 2)
      18)
    (yield
      (column-field :source-id 0 :column-offset 1)
      (column-field :source-id 0 :column-offset 2))))
```

The above syntax more or less matches the query plan's internal data structure.
Like Lisp, it is [homoiconic](http://en.wikipedia.org/wiki/Homoiconicity).

* `scan` iterates through every row in a given table, and runs the provided expression for each row.
* `source-id` is a sort of "variable" that's scoped to the child nodes.
It's an identifier for a row or group.
* `if` evaluates a predicate expression, and runs the second expression if the predicate holds true.
* `column-field` resolves to a variant data type. The source-id identifies either a row or group.
* `yield` invokes a callback in Rust, signaling a row result.
