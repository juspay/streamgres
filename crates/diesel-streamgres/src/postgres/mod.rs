//! What the wrapper knows of PostgreSQL itself: column types, encoding
//! values in its binary format, rows kept after a read, and the cursor
//! Diesel reads them through.

pub mod cursor;
pub mod encode;
pub mod row;
pub mod types;
