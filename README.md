# LlamaDB

[![Build Status](https://travis-ci.org/nukep/llamadb.svg?branch=master)](https://travis-ci.org/nukep/llamadb)

### Fair warning: this project is in the design/implementation phase, and is not stable.
Do not use this for anything important, like... for anything.

LlamaDB is a simple SQL database, written entirely in the Rust programming language.

## Building

LlamaDB is built using the nightly builds of Rust and Cargo.
For installation instructions, go to http://www.rust-lang.org.

To use the CLI, navigate to the `cli/` directory and run `cargo run`.
You'll be greeted by the friendly `llamadb> ` prompt, with whom you can enter SQL queries.
To exit the CLI, press `Ctrl+D`.


## Examples, Features and TODO

For a quick rundown of implemented features, see the [Usage guide](Usage.md).

See the issue tracker for unimplemented features and bugs:
<https://github.com/nukep/llamadb/issues>


## Another SQL database?

Yes.

I started this mostly as a learning project so that I could learn SQL and its
implementation details better. This project may or may not go anywhere; we'll just have to see.

The other reason I started this was to see how the Rust programming language
could be used to write large projects such as SQL databases.
So far, I think it's working out fairly well. :)


## Special thanks

A **HUGE THANKS** goes out to SQLite and the SQLite documentation.
Their wonderful docs helped shed some light on the SQL syntax and other crucial
details such as their B-Tree implementation.
