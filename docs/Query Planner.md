# Query Planner

The query plan is a program that iterates, transforms and yields data from
database tables.

Because the query plan is basically code, it can be represented as a
Lisp-style series of S-expressions.
Lisp notation is what this document will use to illustrate both the internal
data structure of the query plan and how it's executed.

The top-level expression of a query plan must evaluate to a function that yields
the rows returned to the DBMS.


## scan

Iterates through all the rows from a database table.

Syntax: `(scan TABLE SOURCE-ID YIELD-FN)`

If YIELD-FN is not provided, all columns from the table are yielded.

## search

Iterates through a range of rows from a database index.

Syntax: `(search TABLE SOURCE-ID TABLE-INDEX MIN MAX YIELD-FN)`

If YIELD-FN is not provided, all columns from the table are yielded.

## temp-group-by

Syntax: `(temp-group-by YIELD-IN-FN YIELD-GROUP-FN YIELD-OUT-FN)`

## map

Iterates a function that yields rows, and transforms them.
Useful for subqueries.

Syntax: `(map YIELD-IN-FN SOURCE-ID YIELD-OUT-FN)`

## source_id.column_offset

Shorthand for `(column-field source_id column_offset)`.
Returns the matching column from the first row of the source_id.

---


`SELECT name FROM employee;`

    (scan employee A
      (yield A.name))

`SELECT name, albumtitle FROM album, artist WHERE album.artistId = artist.artistId;`

    ; Nested scan. Bad for performance, but requires no optimization rules.
    (scan album A
      (scan artist B
        (if (= A.artistId B.artistId)
          (yield B.name A.albumtitle))))

    ; Nested search. More efficient than a nested scan.
    (scan album A
      (search artist artist_idx B
        (inclusive A.artistId)      ; minimum key
        (inclusive A.artistId)      ; maximum key
        (yield B.name A.albumtitle)))

`SELECT country, count(*) AS population FROM people GROUP BY country;`

    ; Uses a temporary, in-memory table to perform the GROUP BY
    (temp-group-by A
      (scan people B)
      (yield B.country)
      (yield A.country (count A)))

    ; Group by index.
    (search-group-by people_country_idx A
      (yield A.country (count A)))

`SELECT avg(stats.population) FROM (SELECT count(*) AS population FROM people GROUP BY country) stats;`

    (aggregate A
      (temp-group-by B
        (scan people C)                ; ungrouped rows
        (yield C.country)              ; what to group by
        (yield B.country (count B)))   ; output of temp-group-by
      (yield (avg A population)))
