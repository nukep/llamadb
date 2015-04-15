use std::io::Write;
use std::string::ToString;
use std::cmp;
use std::io;

fn string_width(value: &str) -> usize {
    value.split("\n").map(|s| s.chars().count()).max().unwrap()
}

pub fn pretty_select<I, S>(out: &mut Write, column_names: &[String], mut iter: I, page_length: usize)
-> Result<u64, io::Error>
where I: Iterator<Item=Box<[S]>>, S: ToString
{
    debug!("pretty_select");

    let padding = 1;

    let mut row_count = 0;
    loop {
        let mut rows: Vec<Box<[String]>> = Vec::new();

        for _ in 0..page_length {
            match iter.next() {
                Some(n) => {
                    assert_eq!(n.len(), column_names.len());
                    let v: Vec<String> = n.iter().map(|s| s.to_string()).collect();
                    rows.push(v.into_boxed_slice());
                },
                None => break
            }
        }

        row_count += rows.len();

        if rows.is_empty() {
            break;
        } else {
            let widths: Vec<usize> = (0..column_names.len()).map(|i| {
                let row_max = rows.iter().map(|r| string_width(&r[i])).max().unwrap();

                cmp::max(string_width(&column_names[i]), row_max)
            }).collect();

            let table_width = try!(print_headers(out, &widths, padding, column_names));
            for row in rows {
                try!(print_row(out, &widths, padding, &row));
            }
            try!(print_separator(out, table_width));
            try!(write!(out, "\n"));
        }
    }

    if row_count == 0 {
        // no rows; print a table showing "no rows"

        let widths: Vec<usize> = column_names.iter().map(|name| string_width(&name)).collect();

        try!(print_headers(out, &widths, padding, column_names));
        try!(write!(out, "\n"));
    }
    
    Ok(row_count as u64)
}

fn print_headers(out: &mut Write, widths: &[usize], padding: usize, column_names: &[String])
-> Result<usize, io::Error>
{
    // table width
    // = sum(widths) + count(widths) + 1 + count(widths)*2*padding
    // = sum(widths) + count(widths)*(2*padding + 1) + 1
    //
    // ----------------------
    // | AAAA | BB | CCCCCC |
    // ----------------------
    let table_width = widths.iter().fold(0, |prev, &width| prev + width) + widths.len()*(2*padding + 1) + 1;

    try!(print_separator(out, table_width));
    try!(print_row(out, widths, padding, column_names));
    try!(print_separator(out, table_width));

    Ok(table_width)
}

fn print_separator(out: &mut Write, table_width: usize) -> Result<(), io::Error> {
    for _ in 0..table_width {
        try!(write!(out, "-"));
    }
    write!(out, "\n")
}

fn print_row(out: &mut Write, widths: &[usize], padding: usize, columns: &[String])
-> Result<(), io::Error>
{
    for (width, column) in widths.iter().zip(columns.iter()) {
        try!(write!(out, "|"));
        for _ in 0..padding { try!(write!(out, " ")); }
        try!(write!(out, "{}", column));

        for _ in 0..(width-column.chars().count() + padding) { try!(write!(out, " ")); }
    }
    write!(out, "|\n")
}
