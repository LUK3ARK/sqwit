# SQWIT (SQL to WIT Generator)

SQWIT is a Rust library that generates WebAssembly Interface Types (WIT) definitions from SQL table schemas. It provides a simple and efficient way to create WIT representations of your database structures.

## Features

- Parse SQL CREATE TABLE statements
- Generate WIT definitions from SQL schemas
- Support for various SQL data types and constraints
- Easy-to-use API for adding multiple tables and rendering WIT output

## Support

- Supports wasmcloud:postgres implementation (https://github.com/wasmCloud/wasmCloud)
- abstracts and translates all sql possible types into suitable wit primitives and wasmcloud:postgres provided types

## Example

run
```bash
    cargo run --example witify
```
